-- Historical context-keyed aggregates. All scalar business observations remain in SQL.
-- Run only through build_customer_features.py: validated psql variables and staging target.
-- No deduplication of historical event-row counts. TIMESTAMP WITHOUT TIME ZONE.
CREATE TABLE features.:"feature_stage" AS
WITH h AS MATERIALIZED (
 SELECT * FROM raw.gold_events
 WHERE event_datetime < :'snapshot'::timestamp AND nullif(btrim(context_id),'') IS NOT NULL
), ordered AS (
 SELECT *,row_number() OVER(PARTITION BY context_id ORDER BY event_datetime DESC,source_filename DESC,source_row_number DESC) recent_rank FROM h
), g AS (
 SELECT
  context_id AS context_id,
  :'snapshot'::timestamp AS feature_as_of,
  count(*) AS history_row_count,
  min(event_datetime) AS first_observed_event_at,
  max(event_datetime) AS last_observed_event_at,
  extract(epoch FROM (:'snapshot'::timestamp - (max(event_datetime))))/3600.0 AS hours_since_last_observed_event,
  count(*) FILTER(WHERE nullif(btrim(channel),'') IS NULL) AS missing_channel_rows,
  count(*) FILTER(WHERE nullif(btrim(channel),'') IS NOT NULL AND channel NOT IN ('SMS','WA','GRCS')) AS unknown_nonblank_channel_rows,
  count(*) FILTER(WHERE NOT (nullif(btrim(application_id),'') IS NOT NULL)) AS missing_application_rows,
  count(*) FILTER(WHERE NOT (nullif(btrim(session_id),'') IS NOT NULL)) AS missing_session_rows,
  count(*) FILTER(WHERE NOT (nullif(btrim(product_id),'') IS NOT NULL)) AS missing_product_rows,
  count(*) FILTER(WHERE NOT (nullif(btrim(partner_id),'') IS NOT NULL)) AS missing_partner_rows,
  count(*) FILTER(WHERE channel='SMS' AND event_name='campaign_sent') AS sms_send_rows,
  count(*) FILTER(WHERE channel='SMS' AND event_name='campaign_open_read') AS sms_open_read_rows,
  count(*) FILTER(WHERE channel='SMS' AND event_name='campaign_clicked') AS sms_click_rows,
  max(event_datetime) FILTER(WHERE channel='SMS' AND event_name='campaign_sent') AS sms_last_send_at,
  extract(epoch FROM (:'snapshot'::timestamp - (max(event_datetime) FILTER(WHERE channel='SMS' AND event_name='campaign_sent'))))/3600.0 AS sms_hours_since_last_send,
  max(event_datetime) FILTER(WHERE channel='SMS' AND event_name IN ('campaign_open_read','campaign_clicked')) AS sms_last_engagement_at,
  extract(epoch FROM (:'snapshot'::timestamp - (max(event_datetime) FILTER(WHERE channel='SMS' AND event_name IN ('campaign_open_read','campaign_clicked')))))/3600.0 AS sms_hours_since_last_engagement,
  count(*) FILTER(WHERE event_datetime >= :'snapshot'::timestamp - interval '1 days' AND channel='SMS' AND event_name='campaign_sent') AS sms_send_rows_1d,
  count(*) FILTER(WHERE event_datetime >= :'snapshot'::timestamp - interval '7 days' AND channel='SMS' AND event_name='campaign_sent') AS sms_send_rows_7d,
  count(*) FILTER(WHERE event_datetime >= :'snapshot'::timestamp - interval '30 days' AND channel='SMS' AND event_name='campaign_sent') AS sms_send_rows_30d,
  count(*) FILTER(WHERE channel='SMS' AND event_name='campaign_open_read')::double precision/nullif(count(*) FILTER(WHERE channel='SMS' AND event_name='campaign_sent'),0) AS sms_unmatched_open_event_row_ratio,
  NULL::double precision AS sms_matched_open_rate,
  NULL::double precision AS sms_median_time_to_open_hours,
  NULL::double precision AS sms_channel_affinity,
  count(*) FILTER(WHERE channel='WA' AND event_name='campaign_sent') AS wa_send_rows,
  count(*) FILTER(WHERE channel='WA' AND event_name='campaign_open_read') AS wa_open_read_rows,
  count(*) FILTER(WHERE channel='WA' AND event_name='campaign_clicked') AS wa_click_rows,
  max(event_datetime) FILTER(WHERE channel='WA' AND event_name='campaign_sent') AS wa_last_send_at,
  extract(epoch FROM (:'snapshot'::timestamp - (max(event_datetime) FILTER(WHERE channel='WA' AND event_name='campaign_sent'))))/3600.0 AS wa_hours_since_last_send,
  max(event_datetime) FILTER(WHERE channel='WA' AND event_name IN ('campaign_open_read','campaign_clicked')) AS wa_last_engagement_at,
  extract(epoch FROM (:'snapshot'::timestamp - (max(event_datetime) FILTER(WHERE channel='WA' AND event_name IN ('campaign_open_read','campaign_clicked')))))/3600.0 AS wa_hours_since_last_engagement,
  count(*) FILTER(WHERE event_datetime >= :'snapshot'::timestamp - interval '1 days' AND channel='WA' AND event_name='campaign_sent') AS wa_send_rows_1d,
  count(*) FILTER(WHERE event_datetime >= :'snapshot'::timestamp - interval '7 days' AND channel='WA' AND event_name='campaign_sent') AS wa_send_rows_7d,
  count(*) FILTER(WHERE event_datetime >= :'snapshot'::timestamp - interval '30 days' AND channel='WA' AND event_name='campaign_sent') AS wa_send_rows_30d,
  count(*) FILTER(WHERE channel='WA' AND event_name='campaign_open_read')::double precision/nullif(count(*) FILTER(WHERE channel='WA' AND event_name='campaign_sent'),0) AS wa_unmatched_open_event_row_ratio,
  NULL::double precision AS wa_matched_open_rate,
  NULL::double precision AS wa_median_time_to_open_hours,
  NULL::double precision AS wa_channel_affinity,
  count(*) FILTER(WHERE channel='GRCS' AND event_name='campaign_sent') AS grcs_send_rows,
  count(*) FILTER(WHERE channel='GRCS' AND event_name='campaign_open_read') AS grcs_open_read_rows,
  count(*) FILTER(WHERE channel='GRCS' AND event_name='campaign_clicked') AS grcs_click_rows,
  max(event_datetime) FILTER(WHERE channel='GRCS' AND event_name='campaign_sent') AS grcs_last_send_at,
  extract(epoch FROM (:'snapshot'::timestamp - (max(event_datetime) FILTER(WHERE channel='GRCS' AND event_name='campaign_sent'))))/3600.0 AS grcs_hours_since_last_send,
  max(event_datetime) FILTER(WHERE channel='GRCS' AND event_name IN ('campaign_open_read','campaign_clicked')) AS grcs_last_engagement_at,
  extract(epoch FROM (:'snapshot'::timestamp - (max(event_datetime) FILTER(WHERE channel='GRCS' AND event_name IN ('campaign_open_read','campaign_clicked')))))/3600.0 AS grcs_hours_since_last_engagement,
  count(*) FILTER(WHERE event_datetime >= :'snapshot'::timestamp - interval '1 days' AND channel='GRCS' AND event_name='campaign_sent') AS grcs_send_rows_1d,
  count(*) FILTER(WHERE event_datetime >= :'snapshot'::timestamp - interval '7 days' AND channel='GRCS' AND event_name='campaign_sent') AS grcs_send_rows_7d,
  count(*) FILTER(WHERE event_datetime >= :'snapshot'::timestamp - interval '30 days' AND channel='GRCS' AND event_name='campaign_sent') AS grcs_send_rows_30d,
  count(*) FILTER(WHERE channel='GRCS' AND event_name='campaign_open_read')::double precision/nullif(count(*) FILTER(WHERE channel='GRCS' AND event_name='campaign_sent'),0) AS grcs_unmatched_open_event_row_ratio,
  NULL::double precision AS grcs_matched_open_rate,
  NULL::double precision AS grcs_median_time_to_open_hours,
  NULL::double precision AS grcs_channel_affinity,
  count(*) FILTER(WHERE event_datetime >= :'snapshot'::timestamp - interval '1 days' AND channel IN ('SMS','WA','GRCS') AND event_name='campaign_sent') AS send_rows_1d,
  count(*) FILTER(WHERE event_datetime >= :'snapshot'::timestamp - interval '7 days' AND channel IN ('SMS','WA','GRCS') AND event_name='campaign_sent') AS send_rows_7d,
  count(*) FILTER(WHERE event_datetime >= :'snapshot'::timestamp - interval '30 days' AND channel IN ('SMS','WA','GRCS') AND event_name='campaign_sent') AS send_rows_30d,
  count(*) FILTER(WHERE nullif(btrim(channel),'') IS NULL AND event_name='campaign_sent') AS missing_channel_send_rows,
  count(DISTINCT application_id) FILTER(WHERE nullif(btrim(application_id),'') IS NOT NULL) AS distinct_application_count,
  NULL::bigint AS previous_application_count,
  (array_agg(journey_stage ORDER BY event_datetime DESC,source_filename DESC,source_row_number DESC) FILTER(WHERE nullif(btrim(journey_stage),'') IS NOT NULL OR nullif(btrim(journey_substage),'') IS NOT NULL))[1] AS latest_observed_journey_stage,
  (array_agg(journey_substage ORDER BY event_datetime DESC,source_filename DESC,source_row_number DESC) FILTER(WHERE nullif(btrim(journey_stage),'') IS NOT NULL OR nullif(btrim(journey_substage),'') IS NOT NULL))[1] AS latest_observed_journey_substage,
  (array_agg(event_datetime ORDER BY event_datetime DESC,source_filename DESC,source_row_number DESC) FILTER(WHERE nullif(btrim(journey_stage),'') IS NOT NULL OR nullif(btrim(journey_substage),'') IS NOT NULL))[1] AS latest_observed_journey_at,
  extract(epoch FROM (:'snapshot'::timestamp - (max(event_datetime) FILTER(WHERE nullif(btrim(journey_stage),'') IS NOT NULL OR nullif(btrim(journey_substage),'') IS NOT NULL))))/3600.0 AS hours_since_last_journey_event,
  extract(epoch FROM (:'snapshot'::timestamp - (max(event_datetime) FILTER(WHERE event_name IN ('AIP','aip ok','aip  ok','aip not ok')))))/3600.0 AS hours_since_last_aip_event,
  NULL::timestamp AS last_customer_initiated_activity_at,
  NULL::double precision AS hours_since_last_customer_initiated_activity,
  count(DISTINCT session_id) FILTER(WHERE nullif(btrim(session_id),'') IS NOT NULL) AS distinct_session_count,
  count(*) FILTER(WHERE visitnum IS NOT NULL) AS visitnum_recorded_rows,
  max(visitnum) AS max_observed_visitnum,
  count(DISTINCT event_datetime::date) FILTER(WHERE event_datetime >= :'snapshot'::timestamp - interval '7 days' AND TRUE) AS active_days_7d,
  count(DISTINCT event_datetime::date) FILTER(WHERE event_datetime >= :'snapshot'::timestamp - interval '30 days' AND TRUE) AS active_days_30d,
  count(*) FILTER(WHERE event_datetime >= :'snapshot'::timestamp - :trend_days * interval '1 day') AS recent_window_event_rows,
  count(*) FILTER(WHERE event_datetime >= :'snapshot'::timestamp - 2*:trend_days * interval '1 day' AND event_datetime < :'snapshot'::timestamp - :trend_days * interval '1 day') AS prior_window_event_rows,
  count(*) FILTER(WHERE event_datetime >= :'snapshot'::timestamp - :trend_days * interval '1 day') - count(*) FILTER(WHERE event_datetime >= :'snapshot'::timestamp - 2*:trend_days * interval '1 day' AND event_datetime < :'snapshot'::timestamp - :trend_days * interval '1 day') AS event_activity_trend_delta,
  (array_agg(application_id ORDER BY event_datetime DESC,source_filename DESC,source_row_number DESC) FILTER(WHERE nullif(btrim(application_id),'') IS NOT NULL))[1] AS latest_observed_application_id,
  max(event_datetime) FILTER(WHERE nullif(btrim(application_id),'') IS NOT NULL) AS latest_observed_application_at,
  (array_agg(product_id ORDER BY event_datetime DESC,source_filename DESC,source_row_number DESC) FILTER(WHERE nullif(btrim(product_id),'') IS NOT NULL))[1] AS latest_observed_product_id,
  max(event_datetime) FILTER(WHERE nullif(btrim(product_id),'') IS NOT NULL) AS latest_observed_product_at,
  (array_agg(partner_id ORDER BY event_datetime DESC,source_filename DESC,source_row_number DESC) FILTER(WHERE nullif(btrim(partner_id),'') IS NOT NULL))[1] AS latest_observed_partner_id,
  max(event_datetime) FILTER(WHERE nullif(btrim(partner_id),'') IS NOT NULL) AS latest_observed_partner_at,
  count(DISTINCT product_id) FILTER(WHERE nullif(btrim(product_id),'') IS NOT NULL) AS distinct_product_count,
  count(*) FILTER(WHERE nullif(btrim(product_id),'') IS NOT NULL) AS product_associated_rows,
  count(*) FILTER(WHERE event_name='campaign_sent') AS campaign_send_rows,
  count(DISTINCT campaign_name) FILTER(WHERE event_name='campaign_sent' AND nullif(btrim(campaign_name),'') IS NOT NULL) AS distinct_campaign_count,
  count(*) FILTER(WHERE event_name='campaign_sent' AND NOT (nullif(btrim(campaign_name),'') IS NOT NULL)) AS missing_campaign_send_rows,
  count(DISTINCT partner_id) FILTER(WHERE nullif(btrim(partner_id),'') IS NOT NULL) AS distinct_partner_count,
  count(*) FILTER(WHERE nullif(btrim(partner_id),'') IS NOT NULL) AS partner_associated_rows,
  jsonb_agg(jsonb_build_object('event_datetime',event_datetime,'event_name',event_name,'channel',channel,'application_id',application_id,'product_id',product_id,'partner_id',partner_id,'journey_stage',journey_stage,'journey_substage',journey_substage,'source_filename',source_filename,'source_row_number',source_row_number) ORDER BY event_datetime,source_filename,source_row_number) FILTER(WHERE recent_rank<=:sequence_length) AS recent_events
 FROM ordered GROUP BY context_id
), ev AS (
 SELECT context_id,jsonb_object_agg(event_name,n) labels FROM (SELECT context_id,event_name,count(*) n FROM h WHERE event_name IS NOT NULL GROUP BY 1,2) x GROUP BY 1
), ai AS (
 SELECT context_id,jsonb_object_agg(event_name,n) labels,jsonb_object_agg(event_name,extract(epoch FROM (:'snapshot'::timestamp-last_at))/3600.0) recencies
 FROM (SELECT context_id,event_name,count(*) n,max(event_datetime) last_at FROM h WHERE event_name IN ('AIP','aip ok','aip  ok','aip not ok') GROUP BY 1,2) x GROUP BY 1
), ap AS (
 SELECT context_id,jsonb_agg(jsonb_build_object('application_id',application_id,'rows',n,'first_at',first_at,'last_at',last_at,'distinct_products',products,'distinct_partners',partners) ORDER BY application_id) items
 FROM (SELECT context_id,application_id,count(*) n,min(event_datetime) first_at,max(event_datetime) last_at,count(DISTINCT product_id) FILTER(WHERE nullif(btrim(product_id),'') IS NOT NULL) products,count(DISTINCT partner_id) FILTER(WHERE nullif(btrim(partner_id),'') IS NOT NULL) partners FROM h WHERE nullif(btrim(application_id),'') IS NOT NULL GROUP BY 1,2) x GROUP BY 1
), pr AS (
 SELECT context_id,jsonb_agg(jsonb_build_object('product_id',product_id,'rows',n,'last_at',last_at) ORDER BY product_id) items
 FROM (SELECT context_id,product_id,count(*) n,max(event_datetime) last_at FROM h WHERE nullif(btrim(product_id),'') IS NOT NULL GROUP BY 1,2) x GROUP BY 1
), pa AS (
 SELECT context_id,jsonb_agg(jsonb_build_object('partner_id',partner_id,'rows',n,'last_at',last_at) ORDER BY partner_id) items
 FROM (SELECT context_id,partner_id,count(*) n,max(event_datetime) last_at FROM h WHERE nullif(btrim(partner_id),'') IS NOT NULL GROUP BY 1,2) x GROUP BY 1
), ca AS (
 SELECT context_id,jsonb_object_agg(campaign_name,n) labels FROM (SELECT context_id,campaign_name,count(*) n FROM h WHERE event_name='campaign_sent' AND nullif(btrim(campaign_name),'') IS NOT NULL GROUP BY 1,2) x GROUP BY 1
), ib AS (
 SELECT context_id,jsonb_object_agg(initiated_by,n) labels FROM (SELECT context_id,initiated_by,count(*) n FROM h WHERE initiated_by IS NOT NULL GROUP BY 1,2) x GROUP BY 1
), application_quality AS (
 SELECT application_id,count(DISTINCT context_id) contexts,count(*) FILTER(WHERE nullif(btrim(context_id),'') IS NULL) unknown_contexts
 FROM raw.gold_events WHERE event_datetime < :'snapshot'::timestamp AND nullif(btrim(application_id),'') IS NOT NULL GROUP BY 1
), conflicting_applications AS (
 SELECT DISTINCT application_id FROM h WHERE nullif(btrim(application_id),'') IS NOT NULL AND (nullif(btrim(journey_stage),'') IS NOT NULL OR nullif(btrim(journey_substage),'') IS NOT NULL)
 GROUP BY application_id,event_datetime HAVING count(DISTINCT ROW(journey_stage,journey_substage))>1
), stage_observations AS (
 SELECT DISTINCT h.context_id,h.application_id,h.event_datetime,h.journey_stage,h.journey_substage FROM h JOIN application_quality q USING(application_id)
 WHERE q.contexts=1 AND q.unknown_contexts=0 AND NOT EXISTS(SELECT 1 FROM conflicting_applications bad WHERE bad.application_id=h.application_id)
 AND (nullif(btrim(journey_stage),'') IS NOT NULL OR nullif(btrim(journey_substage),'') IS NOT NULL)
), stage_pairs AS (
 SELECT *,lag(event_datetime) OVER w prior_at,lag(journey_stage) OVER w prior_stage,lag(journey_substage) OVER w prior_substage
 FROM stage_observations WINDOW w AS (PARTITION BY context_id,application_id ORDER BY event_datetime)
), tr AS (
 SELECT context_id,count(DISTINCT application_id) eligible_applications,
 CASE WHEN count(*) FILTER(WHERE prior_at IS NOT NULL)>0 THEN count(*) FILTER(WHERE prior_at IS NOT NULL AND ROW(journey_stage,journey_substage) IS DISTINCT FROM ROW(prior_stage,prior_substage)) ELSE NULL END changes
 FROM stage_pairs GROUP BY 1
), jt AS (
 SELECT DISTINCT ON(context_id) context_id,event_datetime,count(DISTINCT ROW(journey_stage,journey_substage)) variants
 FROM h WHERE nullif(btrim(journey_stage),'') IS NOT NULL OR nullif(btrim(journey_substage),'') IS NOT NULL
 GROUP BY context_id,event_datetime ORDER BY context_id,event_datetime DESC
), windows AS (
 SELECT unnest(string_to_array(:'pressure_windows',',')::integer[]) days
), cp AS (
 SELECT context_id,jsonb_object_agg(days,counts) items FROM (
 SELECT context_id,days,jsonb_build_object('SMS',count(*) FILTER(WHERE event_name='campaign_sent' AND event_datetime>=:'snapshot'::timestamp-days*interval '1 day' AND channel='SMS'),'WA',count(*) FILTER(WHERE event_name='campaign_sent' AND event_datetime>=:'snapshot'::timestamp-days*interval '1 day' AND channel='WA'),'GRCS',count(*) FILTER(WHERE event_name='campaign_sent' AND event_datetime>=:'snapshot'::timestamp-days*interval '1 day' AND channel='GRCS'),'across_channels',count(*) FILTER(WHERE event_name='campaign_sent' AND event_datetime>=:'snapshot'::timestamp-days*interval '1 day' AND channel IN('SMS','WA','GRCS')),'missing_channel',count(*) FILTER(WHERE event_name='campaign_sent' AND event_datetime>=:'snapshot'::timestamp-days*interval '1 day' AND nullif(btrim(channel),'') IS NULL)) counts
 FROM h CROSS JOIN windows
 GROUP BY context_id,days) x GROUP BY context_id
), coverage AS (
 SELECT min(event_datetime) first_at,max(event_datetime) last_at,array_agg(DISTINCT source_month ORDER BY source_month) months FROM raw.gold_events WHERE event_datetime < :'snapshot'::timestamp
)
SELECT g.*,
 coalesce(ev.labels,'{}'::jsonb) AS event_label_counts,
 coalesce(ai.labels,'{}'::jsonb) AS aip_label_counts,
 coalesce(ai.recencies,'{}'::jsonb) AS aip_label_recency_hours,
 coalesce(ap.items,'[]'::jsonb) AS application_history,
 coalesce(pr.items,'[]'::jsonb) AS product_history,
 coalesce(pa.items,'[]'::jsonb) AS partner_history,
 coalesce(ca.labels,'{}'::jsonb) AS campaign_send_counts,
 coalesce(ib.labels,'{}'::jsonb) AS initiated_by_counts,
 coalesce(tr.eligible_applications,0)::bigint AS journey_transition_eligible_applications,
 tr.changes AS observed_application_stage_changes,
 g.distinct_application_count-coalesce(tr.eligible_applications,0) AS journey_transition_excluded_applications,
 jt.variants>1 AS latest_journey_timestamp_conflicting,
 coalesce(cp.items,'{}'::jsonb) AS contact_pressure_windows,
 jsonb_build_object('available_history_start',coverage.first_at,'available_history_end',coverage.last_at,'observed_months',coverage.months,'timezone_contract','unresolved naive timestamps','history_is_lifetime',false,'trend_window_days',:trend_days,'trend_windows_within_available_time_span',(:'snapshot'::timestamp-2*:trend_days*interval '1 day'>=coverage.first_at),'recent_event_limit',:sequence_length,'retained_recent_rows',least(g.history_row_count,:sequence_length),'older_rows_in_aggregates',greatest(g.history_row_count-:sequence_length,0),'ordering','timestamp then source filename and row number; ties not causal') AS source_coverage,
 jsonb_build_object('matched_response','unavailable: unverified message linkage and response-window completeness','affinity','unavailable: linkage, observation-window and business parameter contracts','previous_applications','unavailable: current-application selection contract','customer_initiated_activity','unavailable: initiated_by classification contract','visit_semantics','unverified; raw visitnum observations only','stage','latest observed, not authoritative current stage','open_telemetry','unknown: absence does not imply unread or no interest','cost','excluded: external decision tie-breaker') AS feature_availability
FROM g LEFT JOIN ev USING(context_id) LEFT JOIN ai USING(context_id) LEFT JOIN ap USING(context_id)
LEFT JOIN pr USING(context_id) LEFT JOIN pa USING(context_id) LEFT JOIN ca USING(context_id)
LEFT JOIN ib USING(context_id) LEFT JOIN tr USING(context_id) LEFT JOIN jt USING(context_id)
LEFT JOIN cp USING(context_id) CROSS JOIN coverage;
