"""Bounded, local-only psql transport. Credentials never appear in command lines."""
import json
import os
from pathlib import Path
import shutil
import subprocess
import uuid

ROOT = Path(__file__).resolve().parents[2]


class DatabaseUnavailable(RuntimeError):
    """Safe diagnostic without credential values or COPY row contents."""


def configuration(root: Path = ROOT, *, prefer_application: bool = True) -> dict:
    """Read only explicit PG settings; local dotenv values never execute shell code."""
    env = dict(os.environ)
    app_config = root / '.env.postgres.app'
    path = app_config if prefer_application and app_config.exists() else root / '.env.postgres'
    allowed = {'PGHOST','PGPORT','PGDATABASE','PGUSER','PGPASSWORD','PGPASSFILE','PGSSLMODE'}
    if path.exists():
        for line in path.read_text(encoding='utf-8-sig').splitlines():
            if not line.strip() or line.lstrip().startswith('#'):
                continue
            key, separator, value = line.partition('=')
            key = key.strip()
            if not separator or key not in allowed:
                raise ValueError('Local PostgreSQL configuration contains an unsupported key')
            value = value.strip()
            if len(value)>1 and value[0] == value[-1] and value[0] in ('"', "'"):
                value = value[1:-1]
            env.setdefault(key, value)
    env.setdefault('PGHOST', '127.0.0.1')
    env.setdefault('PGPORT', '5432')
    env.setdefault('PGDATABASE', 'laya_testing')
    if env['PGHOST'] not in ('127.0.0.1', 'localhost', '::1') or env['PGPORT'] != '5432':
        raise ValueError('This pipeline permits only the intended local PostgreSQL endpoint')
    if env['PGDATABASE'] != 'laya_testing':
        raise ValueError('Database must be laya_testing')
    # A separately inherited hostaddr must not redirect the explicit local host.
    env.pop('PGHOSTADDR',None)
    if not env.get('PGUSER'):
        raise DatabaseUnavailable('PGUSER is missing; configure .env.postgres or process environment')
    env.update(PGCONNECT_TIMEOUT='5', PGCLIENTENCODING='UTF8', PGAPPNAME='laya_feature_pipeline')
    return env


def executable() -> str:
    found = shutil.which('psql')
    if found:
        return found
    base = Path(r'C:\Program Files\PostgreSQL')
    candidates = sorted(base.glob('*/bin/psql.exe'), key=lambda p: int(p.parents[1].name), reverse=True)
    if not candidates:
        raise DatabaseUnavailable('psql is not installed or not discoverable; package installation requires approval')
    return str(candidates[0])


def command(env: dict, database: str | None = None) -> list[str]:
    return [executable(), '-X', '-w', '--set=ON_ERROR_STOP=1', '-q', '-A', '-t',
            '-h', env['PGHOST'], '-p', env['PGPORT'], '-U', env['PGUSER'],
            '-d', database or env['PGDATABASE']]


def safe_error(stderr: str) -> str:
    """Expose the blocker but not database values, passwords, SQL or COPY context."""
    if 'no password supplied' in stderr:
        return 'PostgreSQL authentication requires PGPASSWORD or a valid PGPASSFILE; no password supplied'
    if 'password authentication failed' in stderr:
        return 'PostgreSQL password authentication failed for the configured account'
    if 'permission denied' in stderr:
        return 'PostgreSQL denied a required database/schema/table permission'
    if 'does not exist' in stderr and 'database' in stderr:
        return 'Configured database does not exist and could not be opened'
    if 'already exists' in stderr:
        return 'A target or staging database object already exists; nothing will be overwritten'
    if 'could not connect' in stderr or 'connection refused' in stderr:
        return 'Cannot connect to the local PostgreSQL server'
    return 'PostgreSQL command failed; detail withheld to prevent exposing COPY row values; inspect locally with psql'


def execute(sql: str, *, env: dict | None = None, database: str | None = None,
            timeout: int = 7200, variables: dict | None = None) -> str:
    env = dict(env or configuration())
    call_name='laya_feature_pipeline_'+uuid.uuid4().hex
    env['PGAPPNAME']=call_name
    args = command(env, database)
    for key,value in (variables or {}).items():
        args.append(f'--set={key}={value}')
    try:
        process = subprocess.run(args, input=sql.encode('utf-8'), stdout=subprocess.PIPE,
                                 stderr=subprocess.PIPE, env=env, timeout=timeout)
    except subprocess.TimeoutExpired as exc:
        # Killing psql alone can leave a CPU-bound server statement running.
        # Cancel only this UUID-labelled connection owned by the same role.
        cancel_env=dict(env,PGAPPNAME='laya_feature_pipeline_cancel')
        cancel_sql="SELECT pg_cancel_backend(pid) FROM pg_stat_activity WHERE application_name='"+call_name+"' AND usename=current_user AND pid<>pg_backend_pid();"
        try:
            subprocess.run(command(cancel_env),input=cancel_sql.encode(),stdout=subprocess.PIPE,
                           stderr=subprocess.PIPE,env=cancel_env,timeout=15)
        except subprocess.TimeoutExpired:
            pass
        raise DatabaseUnavailable('PostgreSQL client deadline exceeded; cancellation requested for this call; inspect server activity before retry') from exc
    if process.returncode:
        raise DatabaseUnavailable(safe_error(process.stderr.decode('utf-8', errors='replace')))
    return process.stdout.decode('utf-8').strip()


def query(sql: str, *, env: dict | None = None, database: str | None = None, timeout: int = 900):
    output = execute(f'SELECT coalesce(json_agg(q),\'[]\'::json) FROM ({sql.rstrip().rstrip(";")}) q;',
                     env=env, database=database, timeout=timeout)
    return json.loads(output)


def ensure_database(env: dict | None = None) -> dict:
    """Create only the named database, with the authenticated account as owner."""
    env = env or configuration()
    details_sql='SELECT version() version,current_database() database,current_user role,inet_server_addr() host,inet_server_port() port'
    try:
        details=query(details_sql,env=env)[0]
    except DatabaseUnavailable as exc:
        if 'database does not exist' not in str(exc):
            raise
        permissions = query('SELECT rolcreatedb OR rolsuper AS allowed FROM pg_roles WHERE rolname=current_user',
                            env=env, database='postgres')[0]
        if not permissions['allowed']:
            raise DatabaseUnavailable('laya_testing is absent and the authenticated role lacks CREATEDB; administrator must create/grant it')
        execute('CREATE DATABASE laya_testing;', env=env, database='postgres', timeout=120)
        details=query(details_sql,env=env)[0]
    details['schemas'] = query("SELECT schema_name FROM information_schema.schemata WHERE schema_name IN ('raw','features') ORDER BY 1", env=env)
    return details


def csv_record(values) -> str:
    """COPY CSV encoding: unquoted empty=NULL; ALL non-NULL values are quoted.

    Quoting preserves empty strings, literal \\N, commas, CR/LF, Unicode and quotes.
    """
    return ','.join('' if value is None else '"' + str(value).replace('"','""') + '"' for value in values) + '\n'
