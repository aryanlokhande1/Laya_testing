"""Isolated HTTP workers with supervisor deadlines and process/runtime diagnostics."""
from datetime import datetime,timezone
import json
import os
from pathlib import Path
import subprocess
import sys
import time


def utc_now():
    return datetime.now(timezone.utc).isoformat()


def wire_bytes(payload):
    return json.dumps(payload,ensure_ascii=False).encode('utf-8')


def process_snapshot():
    if os.name!='nt':
        return {'available':False,'reason':'Windows process diagnostics only','client_pid':os.getpid()}
    command="Get-Process -ErrorAction SilentlyContinue | Where-Object { $_.ProcessName -like '*ollama*' -or $_.ProcessName -like '*llama*' } | Select-Object Id,ProcessName,CPU,WorkingSet64,@{Name='StartTimeUtc';Expression={try {$_.StartTime.ToUniversalTime().ToString('o')} catch {$null}}} | ConvertTo-Json -Depth 3 -Compress"
    try:
        result=subprocess.run(['powershell.exe','-NoProfile','-NonInteractive','-Command',command],
            capture_output=True,text=True,timeout=5,creationflags=subprocess.CREATE_NO_WINDOW)
        return {'available':result.returncode==0,'client_pid':os.getpid(),'processes':json.loads(result.stdout) if result.stdout.strip() else []}
    except Exception as exc:
        return {'available':False,'client_pid':os.getpid(),'error_code':type(exc).__name__}


def timed_out_exception(exc):
    import socket
    seen=set()
    while exc is not None and id(exc) not in seen:
        seen.add(id(exc))
        if isinstance(exc,(TimeoutError,socket.timeout)) or isinstance(getattr(exc,'reason',None),TimeoutError):
            return True
        exc=exc.__cause__ or exc.__context__
    return False


def worker(request_path,result_path,base_url,timeout):
    from http_client import request_json
    result={'worker_pid':os.getpid(),'request_started_utc':utc_now(),'http_timeout_seconds':timeout}
    started=time.perf_counter()
    try:
        payload=json.loads(Path(request_path).read_text(encoding='utf-8'))
        raw,latency=request_json(base_url,'/v1/systemone',payload,timeout=timeout)
        result.update(status='response',response=raw,http_latency_ms=latency)
    except Exception as exc:
        result.update(status='http_timeout' if timed_out_exception(exc) else 'http_error',
                      error_code=type(exc).__name__,private_error=str(exc))
    result.update(request_ended_utc=utc_now(),worker_elapsed_ms=round((time.perf_counter()-started)*1000,3))
    temporary=Path(result_path).with_suffix('.worker.tmp')
    temporary.write_text(json.dumps(result,indent=2),encoding='utf-8')
    temporary.replace(result_path)


def supervise(request_path,result_path,base_url,http_timeout=30,deadline=45):
    if not 0<http_timeout<deadline<=120:
        raise ValueError('Require 0 < HTTP timeout < deadline <=120 seconds')
    started=time.perf_counter()
    start_utc=utc_now()
    command=[sys.executable,str(Path(__file__).resolve()),'--worker',str(request_path),str(result_path),base_url,str(http_timeout)]
    process=subprocess.Popen(command,stdout=subprocess.DEVNULL,stderr=subprocess.DEVNULL,
                             creationflags=subprocess.CREATE_NO_WINDOW if os.name=='nt' else 0)
    try:
        process.wait(timeout=deadline)
    except subprocess.TimeoutExpired:
        process.kill()
        process.wait(timeout=5)
    elapsed=time.perf_counter()-started
    # Check elapsed even when a result exists: suspension can let a late result reach disk.
    if elapsed>deadline:
        result={'status':'deadline_exceeded','late_response_retained':Path(result_path).exists()}
    elif Path(result_path).exists():
        result=json.loads(Path(result_path).read_text(encoding='utf-8'))
    else:
        result={'status':'worker_failed','exit_code':process.returncode}
    result.update(supervisor_started_utc=start_utc,supervisor_ended_utc=utc_now(),
                  supervisor_elapsed_ms=round(elapsed*1000,3),deadline_seconds=deadline,
                  http_timeout_seconds=http_timeout,worker_pid=process.pid)
    return result


if __name__=='__main__':
    if len(sys.argv)==6 and sys.argv[1]=='--worker':
        worker(sys.argv[2],sys.argv[3],sys.argv[4],float(sys.argv[5]))
    else:
        raise SystemExit('Internal HTTP worker; use run_robustness.py')
