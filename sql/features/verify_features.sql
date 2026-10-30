-- Independent PostgreSQL checks. Failure aborts the feature publication transaction.
CREATE TEMP TABLE feature_validation_checks(check_name text PRIMARY KEY,passed boolean NOT NULL) ON COMMIT DROP;
INSERT INTO feature_validation_checks VALUES
('feature_row_count_equals_distinct_eligible_contexts',
 (SELECT count(*) FROM features.:"feature_stage") =
 (SELECT count(DISTINCT context_id) FROM raw.gold_events WHERE event_datetime<:'snapshot'::timestamp AND nullif(btrim(context_id),'') IS NOT NULL)),
('no_blank_or_duplicate_grouping_keys',
 NOT EXISTS(SELECT context_id FROM features.:"feature_stage" GROUP BY context_id HAVING count(*)>1 OR nullif(btrim(context_id),'') IS NULL)),
('all_history_rows_represented',
 (SELECT coalesce(sum(history_row_count),0) FROM features.:"feature_stage") =
 (SELECT count(*) FROM raw.gold_events WHERE event_datetime<:'snapshot'::timestamp AND nullif(btrim(context_id),'') IS NOT NULL)),
('snapshot_and_no_future_sequence_rows',
 NOT EXISTS(SELECT 1 FROM features.:"feature_stage" f CROSS JOIN LATERAL jsonb_array_elements(f.recent_events) e
 WHERE (e->>'event_datetime')::timestamp>=:'snapshot'::timestamp OR f.feature_as_of<>:'snapshot'::timestamp)),
('no_negative_recencies',
 NOT EXISTS(SELECT 1 FROM features.:"feature_stage" f CROSS JOIN LATERAL jsonb_each(to_jsonb(f)) e
 WHERE (e.key LIKE '%hours_since%' OR e.key LIKE '%time_to_open_hours') AND e.value<>'null'::jsonb AND (e.value::text)::numeric<0)),
('sequence_length_and_coverage',
 NOT EXISTS(SELECT 1 FROM features.:"feature_stage" WHERE jsonb_array_length(recent_events)<>least(history_row_count,:sequence_length))),
('matched_and_unapproved_features_are_null',
 NOT EXISTS(SELECT 1 FROM features.:"feature_stage" WHERE sms_matched_open_rate IS NOT NULL OR wa_matched_open_rate IS NOT NULL OR grcs_matched_open_rate IS NOT NULL
 OR sms_channel_affinity IS NOT NULL OR wa_channel_affinity IS NOT NULL OR grcs_channel_affinity IS NOT NULL
 OR sms_median_time_to_open_hours IS NOT NULL OR wa_median_time_to_open_hours IS NOT NULL OR grcs_median_time_to_open_hours IS NOT NULL
 OR previous_application_count IS NOT NULL OR last_customer_initiated_activity_at IS NOT NULL OR hours_since_last_customer_initiated_activity IS NOT NULL)),
('dictionary_complete',
 (SELECT count(*) FROM features.feature_dictionary_stage)=:dictionary_count AND
 (SELECT count(*) FROM information_schema.columns WHERE table_schema='features' AND table_name=:'feature_stage')=:dictionary_count);

WITH independent AS (
 SELECT context_id,count(*) n,
 count(*) FILTER(WHERE channel='SMS' AND event_name='campaign_sent') sms_s,
 count(*) FILTER(WHERE channel='WA' AND event_name='campaign_sent') wa_s,
 count(*) FILTER(WHERE channel='GRCS' AND event_name='campaign_sent') grcs_s,
 count(*) FILTER(WHERE channel='SMS' AND event_name='campaign_open_read') sms_o,
 count(*) FILTER(WHERE channel='WA' AND event_name='campaign_open_read') wa_o,
 count(*) FILTER(WHERE channel='GRCS' AND event_name='campaign_open_read') grcs_o,
 count(*) FILTER(WHERE channel='SMS' AND event_name='campaign_clicked') sms_c,
 count(*) FILTER(WHERE channel='WA' AND event_name='campaign_clicked') wa_c,
 count(*) FILTER(WHERE channel='GRCS' AND event_name='campaign_clicked') grcs_c,
 count(*) FILTER(WHERE nullif(btrim(channel),'') IS NULL) missing,
 count(DISTINCT application_id) FILTER(WHERE nullif(btrim(application_id),'') IS NOT NULL) apps,
 count(DISTINCT session_id) FILTER(WHERE nullif(btrim(session_id),'') IS NOT NULL) sessions,
 count(DISTINCT product_id) FILTER(WHERE nullif(btrim(product_id),'') IS NOT NULL) products,
 count(DISTINCT partner_id) FILTER(WHERE nullif(btrim(partner_id),'') IS NOT NULL) partners,
 count(DISTINCT event_datetime::date) FILTER(WHERE event_datetime>=:'snapshot'::timestamp-interval '7 days') days7,
 count(DISTINCT event_datetime::date) FILTER(WHERE event_datetime>=:'snapshot'::timestamp-interval '30 days') days30,
 count(*) FILTER(WHERE event_name='campaign_sent' AND channel IN('SMS','WA','GRCS') AND event_datetime>=:'snapshot'::timestamp-interval '7 days') sends7
 FROM raw.gold_events WHERE event_datetime<:'snapshot'::timestamp AND nullif(btrim(context_id),'') IS NOT NULL GROUP BY 1
)
INSERT INTO feature_validation_checks
SELECT 'independent_per_context_counts',NOT EXISTS(
 SELECT 1 FROM independent i FULL JOIN features.:"feature_stage" f USING(context_id)
 WHERE ROW(i.n,i.sms_s,i.wa_s,i.grcs_s,i.sms_o,i.wa_o,i.grcs_o,i.sms_c,i.wa_c,i.grcs_c,i.missing,i.apps,i.sessions,i.products,i.partners,i.days7,i.days30,i.sends7)
 IS DISTINCT FROM ROW(f.history_row_count,f.sms_send_rows,f.wa_send_rows,f.grcs_send_rows,f.sms_open_read_rows,f.wa_open_read_rows,f.grcs_open_read_rows,f.sms_click_rows,f.wa_click_rows,f.grcs_click_rows,f.missing_channel_rows,f.distinct_application_count,f.distinct_session_count,f.distinct_product_count,f.distinct_partner_count,f.active_days_7d,f.active_days_30d,f.send_rows_7d));

WITH labels AS (
 SELECT context_id,event_name,count(*) n FROM raw.gold_events WHERE event_datetime<:'snapshot'::timestamp AND nullif(btrim(context_id),'') IS NOT NULL
 AND event_name IN('AIP','aip ok','aip  ok','aip not ok') GROUP BY 1,2
), maps AS (SELECT context_id,jsonb_object_agg(event_name,n) labels FROM labels GROUP BY 1)
INSERT INTO feature_validation_checks
SELECT 'exact_separate_aip_labels',NOT EXISTS(SELECT 1 FROM features.:"feature_stage" f LEFT JOIN maps m USING(context_id) WHERE f.aip_label_counts IS DISTINCT FROM coalesce(m.labels,'{}'::jsonb));

DO $validate$
BEGIN
 IF EXISTS(SELECT 1 FROM feature_validation_checks WHERE NOT passed) THEN
 RAISE EXCEPTION 'Essential feature validation failed; transaction aborted';
 END IF;
END $validate$;
SELECT json_build_object('check',check_name,'passed',passed) FROM feature_validation_checks ORDER BY check_name;
