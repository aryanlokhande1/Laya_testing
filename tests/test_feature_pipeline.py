"""Synthetic fixtures only; optional local PostgreSQL integration is transactional."""
from collections import Counter
from datetime import datetime,timedelta
import csv
import io
import json
import math
import os
from pathlib import Path
import sys
import tempfile
import unittest
from unittest.mock import patch

import duckdb

sys.path.insert(0,str(Path(__file__).resolve().parents[1]/'src'))
from features.formulas import (matched_time_to_open_hours,channel_median_time_to_open_hours,
    matched_open_rate,delay_decay,sparse_observation_smoothing,opening_weighted_channel_affinity)
from features.build_customer_features import validate_options
from data.combine_parquets import consolidate,digest,SOURCES,ROOT
from db.postgres import csv_record,configuration,safe_error,execute,DatabaseUnavailable
from db.binary_copy import HEADER,TRAILER,binary_record

FIELDS=[('application_id','VARCHAR'),('context_id','VARCHAR'),('event_datetime','TIMESTAMP_NS'),
 ('event_name','VARCHAR'),('journey_stage','VARCHAR'),('journey_substage','VARCHAR'),
 ('site_subsection','VARCHAR'),('channel_id','VARCHAR'),('platform_type','VARCHAR'),
 ('product_id','VARCHAR'),('partner_id','VARCHAR'),('trigger_type','VARCHAR'),('time_bucket','VARCHAR'),
 ('visitnum','DOUBLE'),('utm_source','VARCHAR'),('utm_medium','VARCHAR'),('utm_campaign','VARCHAR'),
 ('source_type','VARCHAR'),('campaign_name','VARCHAR'),('session_id','VARCHAR'),('channel','VARCHAR'),
 ('utm_channel','VARCHAR'),('day_of_week_num','BIGINT'),('initiated_by','VARCHAR')]


class FormulaTests(unittest.TestCase):
    def test_verified_matched_delay_and_missing(self):
        t=datetime(2026,4,1)
        self.assertEqual(matched_time_to_open_hours(t,t+timedelta(hours=2),linkage_verified=True),2)
        self.assertIsNone(matched_time_to_open_hours(t,t,linkage_verified=False))
        self.assertIsNone(matched_time_to_open_hours(None,t,linkage_verified=True))
        with self.assertRaises(ValueError):
            matched_time_to_open_hours(t,t-timedelta(seconds=1),linkage_verified=True)

    def test_median_uses_only_valid_observed_delays(self):
        self.assertEqual(channel_median_time_to_open_hours([None,1,3],linkage_verified=True),2)
        self.assertIsNone(channel_median_time_to_open_hours([],linkage_verified=True))
        self.assertIsNone(channel_median_time_to_open_hours([1],linkage_verified=False))
        for bad in (-1,float('nan'),float('inf')):
            with self.assertRaises(ValueError):
                channel_median_time_to_open_hours([bad],linkage_verified=True)

    def test_unique_rate_requires_linkage_and_complete_exposure(self):
        self.assertEqual(matched_open_rate(2,4,linkage_verified=True,observation_window_complete=True),.5)
        for verified,complete,n in [(False,True,4),(True,False,4),(True,True,0)]:
            self.assertIsNone(matched_open_rate(0,n,linkage_verified=verified,observation_window_complete=complete))
        for opened,sends in [(3,2),(-1,3),(True,2),(1.5,3)]:
            with self.assertRaises(ValueError):
                matched_open_rate(opened,sends,linkage_verified=True,observation_window_complete=True)

    def test_delay_decay_monotonic_and_half_life(self):
        self.assertEqual(delay_decay(0,half_life_hours=24),1)
        self.assertEqual(delay_decay(24,half_life_hours=24),.5)
        self.assertLess(delay_decay(48,half_life_hours=24),delay_decay(24,half_life_hours=24))
        for delay,half in [(-1,24),(2,0),(2,float('nan'))]:
            with self.assertRaises(ValueError):
                delay_decay(delay,half_life_hours=half)

    def test_sparse_smoothing_and_zero_observations(self):
        self.assertAlmostEqual(sparse_observation_smoothing(.5,4,prior_score=.2,prior_strength=2),.4)
        self.assertIsNone(sparse_observation_smoothing(.5,0,prior_score=.2,prior_strength=2))
        self.assertIsNone(sparse_observation_smoothing(None,4,prior_score=.2,prior_strength=2))
        self.assertLess(sparse_observation_smoothing(.8,2,prior_score=.2,prior_strength=5),sparse_observation_smoothing(.8,20,prior_score=.2,prior_strength=5))
        with self.assertRaises(ValueError):
            sparse_observation_smoothing(.5,4,prior_score=2,prior_strength=2)

    def test_affinity_opening_primary_delay_secondary_and_explicit_parameters(self):
        options=dict(opening_weight=.75,half_life_hours=24,prior_score=.2,prior_strength=2,linkage_verified=True,observation_window_complete=True)
        actual=opening_weighted_channel_affinity(2,4,[0,24],**options)
        self.assertAlmostEqual(actual,(4*(.5*(.75+.25*.75))+2*.2)/6)
        self.assertLess(opening_weighted_channel_affinity(2,4,[48,48],**options),actual)
        self.assertIsNone(opening_weighted_channel_affinity(0,0,[],**options))
        self.assertAlmostEqual(opening_weighted_channel_affinity(0,4,[],**options),.4/6)
        for change in ({'opening_weight':.5},{'half_life_hours':0},{'prior_strength':-1}):
            with self.assertRaises(ValueError):
                opening_weighted_channel_affinity(2,4,[0,24],**(options|change))
        with self.assertRaises(ValueError):
            opening_weighted_channel_affinity(2,4,[0],**options)
        self.assertIsNone(opening_weighted_channel_affinity(2,4,[0,24],**(options|{'linkage_verified':False})))


class PipelineSafetyTests(unittest.TestCase):
    def test_csv_null_empty_unicode_newline_and_quote_preservation(self):
        row=[None,'',r'\N','text,"quoted"\r\nline','नमस्ते',2.5]
        text=csv_record(row)
        self.assertTrue(text.startswith(',"",'))
        self.assertEqual(next(csv.reader(io.StringIO(text))),['','','\\N','text,"quoted"\r\nline','नमस्ते','2.5'])
        # DuckDB emulates the PostgreSQL distinction: only unquoted empties are NULL.
        with tempfile.TemporaryDirectory() as tmp:
            p=Path(tmp)/'x.csv';p.write_bytes(text.encode('utf-8'))
            c=duckdb.connect()
            result=c.execute("SELECT * FROM read_csv(?,header=false,all_varchar=true,nullstr='',allow_quoted_nulls=false,delim=',',quote='\"',escape='\"',new_line='\\n')",[str(p)]).fetchone()
            c.close()
            self.assertEqual(result[:5],tuple(row[:5]))

    def test_configuration_rejects_remote_endpoint_and_secret_in_errors(self):
        with tempfile.TemporaryDirectory() as tmp,patch.dict(os.environ,{'PGHOST':'remote.invalid','PGUSER':'test'},clear=True):
            with self.assertRaises(ValueError):configuration(Path(tmp))
        self.assertNotIn('PRIVATE_ROW',safe_error('ERROR bad COPY PRIVATE_ROW secret'))
        self.assertIn('password',safe_error('password authentication failed'))

    def test_snapshot_and_window_configuration(self):
        self.assertEqual(validate_options('2026-05-01 00:00:00',8,7,[1,7,30]),'2026-05-01 00:00:00')
        for at,n,d,windows in [('2026-05-01T00:00:00+00:00',8,7,[1]),('2026-05-01',0,7,[1]),('2026-05-01',8,0,[1]),('2026-05-01',8,7,[1,1])]:
            with self.assertRaises(ValueError):validate_options(at,n,d,windows)

    def test_lossless_consolidation_exact_cross_file_duplicates_and_source_hashes(self):
        with tempfile.TemporaryDirectory() as tmp:
            root=Path(tmp);(root/'input').mkdir()
            c=duckdb.connect()
            c.execute('CREATE TABLE x('+','.join(n+' '+t for n,t in FIELDS)+')')
            c.execute("INSERT INTO x(context_id,event_datetime,event_name,channel,application_id,session_id,partner_id) VALUES ('fixture','2026-03-01','AIP','SMS','app',NULL,''),('fixture','2026-03-02','aip  ok','WA',NULL,'s','p')")
            for month,name in SOURCES:
                c.execute('COPY x TO ? (FORMAT PARQUET)',[str(root/'input'/name)])
            before={n:digest(root/'input'/n) for _,n in SOURCES}
            result=consolidate(root,root/'reports')
            self.assertEqual(result['combined_rows'],4)
            self.assertTrue(result['exact_multiset_verified'])
            self.assertEqual(result['duplicate_audit']['cross_file_groups'],2)
            self.assertEqual(result['duplicate_audit']['cross_file_matched_pairs'],2)
            self.assertEqual(result['duplicate_audit']['excess_rows'],2)
            self.assertEqual(before,{n:digest(root/'input'/n) for _,n in SOURCES})
            self.assertEqual(result['quality']['nulls']['session_id'],2)
            self.assertEqual(result['quality']['blanks']['partner_id'],2)
            schema=c.execute('DESCRIBE SELECT * FROM read_parquet(?)',[result['consolidated_path']]).fetchall()
            self.assertEqual([(r[0],r[1]) for r in schema[:24]],FIELDS)
            with self.assertRaises(FileExistsError):consolidate(root,root/'reports2')

    def test_dictionary_covers_all_18_families_without_invented_channel(self):
        specs=json.loads((ROOT/'sql/features/feature_definitions.json').read_text())
        self.assertEqual(set(range(1,19)),{x['feature_family'] for x in specs}-{0})
        self.assertEqual(len(specs),len({x['column_name'] for x in specs}))
        self.assertFalse(any('email' in x['column_name'] for x in specs))
        self.assertTrue(all(all(field in x for field in ['meaning','source_fields','formula','implementation','observability','limitations','relevance_to_laya']) for x in specs))


@unittest.skipUnless(os.environ.get('LAYA_TEST_POSTGRES')=='1','Set LAYA_TEST_POSTGRES=1 to run rollback-only local PostgreSQL integration')
class PostgreSQLFeatureTests(unittest.TestCase):
    def test_client_deadline_cancels_only_its_own_server_statement(self):
        with self.assertRaisesRegex(DatabaseUnavailable,'deadline exceeded'):
            execute('SELECT pg_sleep(5);',timeout=1)
        from db.postgres import query
        remaining=query("SELECT count(*) n FROM pg_stat_activity WHERE application_name LIKE 'laya_feature_pipeline_%' AND pid<>pg_backend_pid() AND state='active' AND query='SELECT pg_sleep(5);'")[0]['n']
        self.assertEqual(remaining,0)

    def test_native_binary_copy_preserves_nulls_newlines_unicode_and_timestamps(self):
        schema=[('v','VARCHAR'),('empty','VARCHAR'),('missing','VARCHAR'),('n','BIGINT'),('x','DOUBLE'),('at','TIMESTAMP_NS')]
        value='text,"quoted"\r\nline\nनमस्ते\n\\.\nend'
        with tempfile.TemporaryDirectory() as tmp:
            path=Path(tmp)/'fixture.bin'
            path.write_bytes(HEADER+binary_record([value,'',None,2**40,2.5,'1999-12-31 23:59:59.123456'],schema)+TRAILER)
            script="BEGIN; CREATE TEMP TABLE binary_fixture(v text,empty text,missing text,n bigint,x double precision,at timestamp);\n"
            script+="\\copy binary_fixture FROM '"+path.as_posix()+"' WITH (FORMAT BINARY)\n"
            script+="SELECT json_build_object('hex',encode(convert_to(v,'UTF8'),'hex'),'empty',empty,'missing',missing,'n',n,'x',x,'at',at::text) FROM binary_fixture; ROLLBACK;"
            output=execute(script)
            actual=json.loads(next(line for line in output.splitlines() if line.startswith('{')))
            self.assertEqual(actual,{'hex':value.encode().hex(),'empty':'','missing':None,'n':2**40,'x':2.5,'at':'1999-12-31 23:59:59.123456'})
        with self.assertRaises(ValueError):binary_record(['2026-04-01 00:00:00.000000001'],[('at','TIMESTAMP_NS')])

    def test_real_postgres_sql_with_ties_duplicates_missingness_and_future_cutoff(self):
        types={'VARCHAR':'text','TIMESTAMP_NS':'timestamp','DOUBLE':'double precision','BIGINT':'bigint'}
        schema=','.join(n+' '+types[t] for n,t in FIELDS)+',source_filename text,source_month text,source_row_number bigint'
        script='BEGIN; CREATE TEMP TABLE fixture_events('+schema+');\n'
        script+="""INSERT INTO fixture_events(context_id,event_datetime,event_name,channel,application_id,journey_stage,journey_substage,source_filename,source_month,source_row_number) VALUES
('X','2026-04-01','campaign_sent','SMS','A','Start','s','march','2026-03',1),
('X','2026-04-01','campaign_sent','SMS','A','Start','s','april','2026-04',1),
('X','2026-04-02','campaign_open_read','SMS','A','Middle','s','april','2026-04',2),
('X','2026-04-29','campaign_sent','WA','A','End','s','april','2026-04',3),
('X','2026-04-30 01:00','AIP','GRCS','A','End','s','april','2026-04',4),
('X','2026-04-30 02:00','aip ok','WA','A','End','s','april','2026-04',5),
('X','2026-04-30 03:00','aip  ok','WA','A','End','s','april','2026-04',6),
('X','2026-04-30 04:00','aip not ok',NULL,'A','End','s','april','2026-04',7),
('X','2026-05-01','future','WA','A','Future','s','april','2026-04',8),
('Y','2026-04-01','observed',NULL,'A',NULL,NULL,'april','2026-04',9),
('Z','2026-03-01','observed',NULL,'B','First','s','march','2026-03',2),
('Z','2026-04-01','observed',NULL,'B','Next','s','april','2026-04',10),
('T','2026-04-01','observed',NULL,'C','First','s','april','2026-04',11),
('T','2026-04-01','observed',NULL,'C','Other','s','april','2026-04',12),
('   ','2026-04-01','observed',NULL,NULL,NULL,NULL,'april','2026-04',13),
('N',NULL,'undated',NULL,NULL,NULL,NULL,'april','2026-04',14);
"""
        sql=(ROOT/'sql/features/customer_aggregates.sql').read_text().replace('raw.gold_events','pg_temp.fixture_events').replace('CREATE TABLE features.:"feature_stage" AS','CREATE TEMP TABLE fixture_features AS')
        script+=sql
        script+="CREATE TEMP TABLE fixture_dictionary AS SELECT column_name FROM information_schema.columns WHERE table_schema=(SELECT nspname FROM pg_namespace WHERE oid=pg_my_temp_schema()) AND table_name='fixture_features';\n"
        validation=(ROOT/'sql/features/verify_features.sql').read_text(encoding='utf-8')
        validation=validation.replace('raw.gold_events','pg_temp.fixture_events').replace('features.:"feature_stage"','pg_temp.fixture_features').replace('features.feature_dictionary_stage','pg_temp.fixture_dictionary')
        validation=validation.replace("table_schema='features' AND table_name=:'feature_stage'","table_schema=(SELECT nspname FROM pg_namespace WHERE oid=pg_my_temp_schema()) AND table_name='fixture_features'")
        script+=validation
        script+="""DO $check$ DECLARE x record; z record; t record; BEGIN
 SELECT * INTO x FROM fixture_features WHERE context_id='X';
 SELECT * INTO z FROM fixture_features WHERE context_id='Z';
 SELECT * INTO t FROM fixture_features WHERE context_id='T';
 IF (SELECT count(*) FROM fixture_features)<>4 OR x.history_row_count<>8 OR x.sms_send_rows<>2 OR x.sms_open_read_rows<>1 OR x.wa_send_rows<>1 OR x.missing_channel_rows<>1 OR x.distinct_session_count<>0 THEN RAISE EXCEPTION 'count assertions'; END IF;
 IF x.send_rows_7d<>1 OR x.active_days_7d<>2 OR x.latest_observed_journey_stage<>'End' OR x.hours_since_last_observed_event<>20 THEN RAISE EXCEPTION 'recency assertions'; END IF;
 IF x.previous_application_count IS NOT NULL OR x.sms_matched_open_rate IS NOT NULL OR x.wa_channel_affinity IS NOT NULL OR x.last_customer_initiated_activity_at IS NOT NULL THEN RAISE EXCEPTION 'contract assertions'; END IF;
 IF x.aip_label_counts <> '{"AIP":1,"aip ok":1,"aip  ok":1,"aip not ok":1}'::jsonb THEN RAISE EXCEPTION 'AIP preservation'; END IF;
 IF jsonb_array_length(x.recent_events)<>8 OR x.observed_application_stage_changes IS NOT NULL OR z.observed_application_stage_changes<>1 OR NOT t.latest_journey_timestamp_conflicting OR t.observed_application_stage_changes IS NOT NULL THEN RAISE EXCEPTION 'sequence reliability'; END IF;
 IF x.contact_pressure_windows->'7'->>'WA'<>'1' OR z.contact_pressure_windows->'7'->>'WA'<>'0' THEN RAISE EXCEPTION 'pressure window zeros'; END IF;
 END $check$; SELECT 'integration_verified'; ROLLBACK;
"""
        output=execute(script,variables={'snapshot':'2026-05-01','sequence_length':8,'trend_days':7,'pressure_windows':'1,7,30','dictionary_count':104})
        self.assertIn('integration_verified',output)


if __name__=='__main__':unittest.main()
