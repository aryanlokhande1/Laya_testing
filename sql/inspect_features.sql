-- Run in DBeaver connected to local laya_testing. Customer rows stay local.
SELECT version(),current_database(),current_user;
SELECT count(*) AS raw_rows FROM raw.gold_events;
SELECT count(*) AS feature_rows,count(DISTINCT context_id) AS unique_context_keys
FROM features.customer_features;

SELECT source_filename,source_month,count(*) AS rows,
 min(source_row_number),max(source_row_number),min(event_datetime),max(event_datetime)
FROM raw.gold_events GROUP BY 1,2 ORDER BY 1;

-- Only these observed channels are approved; blank/NULL remain missing data.
SELECT channel,count(*) AS rows FROM raw.gold_events GROUP BY 1 ORDER BY 1;
SELECT * FROM raw.gold_events ORDER BY source_filename,source_row_number LIMIT 20;
SELECT * FROM features.customer_features ORDER BY context_id LIMIT 20;
SELECT * FROM features.feature_dictionary ORDER BY feature_family,column_name;

-- Complete scalar/JSON feature SQL NULL coverage.
SELECT e.key AS column_name,count(*) AS total_rows,
 count(*) FILTER(WHERE e.value='null'::jsonb) AS null_rows
FROM features.customer_features f CROSS JOIN LATERAL jsonb_each(to_jsonb(f)) e
GROUP BY 1 ORDER BY 1;

-- Independent count, cutoff and grouping checks: use the actual configured snapshot.
WITH expected AS (
 SELECT count(DISTINCT context_id) AS contexts,count(*) AS rows
 FROM raw.gold_events WHERE event_datetime<timestamp '2026-05-01 00:00:00'
 AND nullif(btrim(context_id),'') IS NOT NULL
)
SELECT expected.*,(SELECT count(*) FROM features.customer_features) actual_contexts,
 (SELECT sum(history_row_count) FROM features.customer_features) represented_rows FROM expected;

SELECT count(*) AS future_sequence_rows
FROM features.customer_features f CROSS JOIN LATERAL jsonb_array_elements(f.recent_events) e
WHERE (e->>'event_datetime')::timestamp>=f.feature_as_of;

SELECT schemaname,tablename,indexname,indexdef FROM pg_indexes
WHERE (schemaname='raw' AND tablename='gold_events')
OR (schemaname='features' AND tablename IN('customer_features','feature_dictionary')) ORDER BY 1,2,3;

-- Lookup one real context locally; never paste raw customer rows into shared logs.
-- SELECT * FROM features.customer_features WHERE context_id = '<local-context-key>';
