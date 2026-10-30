-- V1 only: all original tables are read-only. No source rows are deduplicated.
-- Existing channel/AIP/activity aggregates are reused; journey selection is corrected here.
-- Class ranks use physical provenance solely to make timestamp ties reproducible.
CREATE TEMP TABLE _state_rollup ON COMMIT DROP AS
WITH classified AS (
 SELECT context_id,event_datetime,event_name,channel,application_id,journey_stage,journey_substage,
  partner_id,source_filename,source_row_number,
  CASE WHEN event_name IN ('campaign_sent','campaign_open_read','campaign_clicked') THEN 'communication'
       WHEN nullif(btrim(event_name),'') IS NOT NULL THEN 'non_campaign' ELSE 'unclassified' END category,
  CASE WHEN nullif(btrim(journey_stage),'') IS NOT NULL AND lower(btrim(journey_stage)) NOT IN ('campaign_sent','campaign_open_read','campaign_clicked') THEN journey_stage ELSE NULL END clean_stage,CASE WHEN nullif(btrim(journey_substage),'') IS NOT NULL AND lower(btrim(journey_substage)) NOT IN ('campaign_sent','campaign_open_read','campaign_clicked') THEN journey_substage ELSE NULL END clean_substage
 FROM raw.gold_events WHERE event_datetime<:'snapshot'::timestamp AND nullif(btrim(context_id),'') IS NOT NULL
), ranked AS (
 SELECT *,row_number() OVER(PARTITION BY context_id,category ORDER BY event_datetime DESC,source_filename DESC,source_row_number DESC) category_rank
 FROM classified
)
SELECT context_id,
 count(*) FILTER(WHERE category='communication') communication_rows,
 count(*) FILTER(WHERE category='non_campaign') non_campaign_rows,
 count(*) FILTER(WHERE category='unclassified') unclassified_rows,
 count(*) FILTER(WHERE channel IS NULL) null_channel_rows,
 count(*) FILTER(WHERE channel IS NOT NULL AND btrim(channel)='') blank_channel_rows,
 count(*) FILTER(WHERE event_name='campaign_sent' AND nullif(btrim(channel),'') IS NULL AND event_datetime>=:'snapshot'::timestamp-interval '7 days') missing_channel_send_rows_7d,
 count(*) FILTER(WHERE partner_id ~ '[|,;:/]') partner_rows_with_separators,
 (array_agg(event_name ORDER BY event_datetime DESC,source_filename DESC,source_row_number DESC) FILTER(WHERE category_rank=1))[1] latest_observed_event_name,
 max(event_name) FILTER(WHERE category='non_campaign' AND category_rank=1) latest_non_campaign_event_name,
 max(clean_stage) FILTER(WHERE category='non_campaign' AND category_rank=1) latest_non_campaign_journey_stage,
 max(clean_substage) FILTER(WHERE category='non_campaign' AND category_rank=1) latest_non_campaign_journey_substage,
 max(event_datetime) FILTER(WHERE category='non_campaign' AND category_rank=1) latest_non_campaign_at,
 bool_or(category='non_campaign' AND category_rank=1 AND nullif(btrim(journey_stage),'') IS NOT NULL AND clean_stage IS NULL) latest_stage_token_rejected,
 bool_or(category='non_campaign' AND category_rank=1 AND nullif(btrim(journey_substage),'') IS NOT NULL AND clean_substage IS NULL) latest_substage_token_rejected,
 coalesce(jsonb_agg(jsonb_build_object('event_name',event_name,'channel',channel,'hours_before_snapshot',extract(epoch FROM (:'snapshot'::timestamp-event_datetime))/3600.0) ORDER BY event_datetime,source_filename,source_row_number) FILTER(WHERE category='communication' AND category_rank<=:communication_limit),'[]'::jsonb) recent_communication_events,
 coalesce(jsonb_agg(jsonb_build_object('event_name',event_name,'channel',channel,'hours_before_snapshot',extract(epoch FROM (:'snapshot'::timestamp-event_datetime))/3600.0,'stage',clean_stage,'substage',clean_substage) ORDER BY event_datetime,source_filename,source_row_number) FILTER(WHERE category='non_campaign' AND category_rank<=:journey_limit),'[]'::jsonb) recent_non_campaign_journey_events,
 jsonb_build_object(
  'selected_events',jsonb_agg(jsonb_build_object('source_filename',source_filename,'source_row_number',source_row_number,'event_datetime',event_datetime,'application_id',application_id,'event_name',event_name,'raw_stage',journey_stage,'raw_substage',journey_substage,'category',category,'category_rank',category_rank) ORDER BY event_datetime,source_filename,source_row_number) FILTER(WHERE (category='communication' AND category_rank<=:communication_limit) OR (category='non_campaign' AND category_rank<=:journey_limit) OR (category='unclassified' AND category_rank=1)),
  'latest_observed_event',(jsonb_agg(jsonb_build_object('source_filename',source_filename,'source_row_number',source_row_number,'event_datetime',event_datetime,'application_id',application_id,'event_name',event_name,'raw_stage',journey_stage,'raw_substage',journey_substage,'category',category,'category_rank',category_rank) ORDER BY event_datetime DESC,source_filename DESC,source_row_number DESC) FILTER(WHERE category_rank=1))->0,
  'latest_non_campaign_event',(jsonb_agg(jsonb_build_object('source_filename',source_filename,'source_row_number',source_row_number,'event_datetime',event_datetime,'application_id',application_id,'event_name',event_name,'raw_stage',journey_stage,'raw_substage',journey_substage,'category',category,'category_rank',category_rank) ORDER BY event_datetime DESC,source_filename DESC,source_row_number DESC) FILTER(WHERE category='non_campaign' AND category_rank=1))->0
 ) event_audit,
 count(*) FILTER(WHERE (category='communication' AND category_rank<=:communication_limit) OR (category='non_campaign' AND category_rank<=:journey_limit) OR (category='unclassified' AND category_rank=1))>count(DISTINCT event_datetime) FILTER(WHERE (category='communication' AND category_rank<=:communication_limit) OR (category='non_campaign' AND category_rank<=:journey_limit) OR (category='unclassified' AND category_rank=1)) audit_selection_has_timestamp_ties
FROM ranked GROUP BY context_id;
ALTER TABLE _state_rollup ADD PRIMARY KEY(context_id);
ANALYZE _state_rollup;

CREATE TABLE features.laya_state_v1 AS
WITH base AS (
 SELECT f.context_id,f.feature_as_of,'laya_state_v1'::text state_schema_version,
 f.sms_send_rows AS sms_send_rows,
 f.sms_open_read_rows AS sms_open_read_rows,
 f.sms_click_rows AS sms_click_rows,
 f.sms_hours_since_last_send AS sms_hours_since_last_send,
 f.sms_hours_since_last_engagement AS sms_hours_since_last_engagement,
 f.sms_send_rows_7d AS sms_send_rows_7d,
 f.wa_send_rows AS wa_send_rows,
 f.wa_open_read_rows AS wa_open_read_rows,
 f.wa_click_rows AS wa_click_rows,
 f.wa_hours_since_last_send AS wa_hours_since_last_send,
 f.wa_hours_since_last_engagement AS wa_hours_since_last_engagement,
 f.wa_send_rows_7d AS wa_send_rows_7d,
 f.grcs_send_rows AS grcs_send_rows,
 f.grcs_open_read_rows AS grcs_open_read_rows,
 f.grcs_click_rows AS grcs_click_rows,
 f.grcs_hours_since_last_send AS grcs_hours_since_last_send,
 f.grcs_hours_since_last_engagement AS grcs_hours_since_last_engagement,
 f.grcs_send_rows_7d AS grcs_send_rows_7d,
 f.history_row_count AS history_row_count,
 extract(epoch FROM (f.last_observed_event_at-f.first_observed_event_at))/3600.0 AS observed_history_hours,
 f.missing_channel_rows AS missing_channel_rows,
 r.missing_channel_send_rows_7d AS missing_channel_send_rows_7d,
 f.send_rows_7d AS send_rows_7d,
 f.distinct_application_count AS distinct_application_count,
 f.distinct_product_count AS distinct_product_count,
 f.hours_since_last_observed_event AS hours_since_last_observed_event,
 r.latest_observed_event_name AS latest_observed_event_name,
 r.latest_non_campaign_event_name AS latest_non_campaign_event_name,
 r.latest_non_campaign_journey_stage AS latest_non_campaign_journey_stage,
 r.latest_non_campaign_journey_substage AS latest_non_campaign_journey_substage,
 extract(epoch FROM (:'snapshot'::timestamp-r.latest_non_campaign_at))/3600.0 AS hours_since_last_non_campaign_event,
 f.active_days_7d AS observed_event_active_days_7d,
 f.aip_label_counts,f.aip_label_recency_hours,r.recent_communication_events,r.recent_non_campaign_journey_events,r.event_audit,
 r.communication_rows _communication_rows,r.non_campaign_rows _non_campaign_rows,
 jsonb_build_object(
  'identity_is_verified_customer_id',false,'current_application_verified',false,
  'user_initiated_mapping_verified',false,'message_matching_verified',false,'response_window_complete_verified',false,
  'affinity_parameters_approved',false,'timezone_contract_resolved',false,
  'communication_limit',:communication_limit,'journey_limit',:journey_limit,
  'limits_are_representation_parameters',true,'unclassified_event_rows',r.unclassified_rows,
  'missing_channel_rows',jsonb_build_object('null',r.null_channel_rows,'blank',r.blank_channel_rows),
  'partner_rows_with_separators',r.partner_rows_with_separators,'partner_encoding_verified',false,
  'latest_non_campaign_available',r.non_campaign_rows>0,
  'latest_non_campaign_stage_unavailable',r.latest_non_campaign_journey_stage IS NULL AND r.latest_non_campaign_journey_substage IS NULL,
  'latest_stage_token_rejected',r.latest_stage_token_rejected,'latest_substage_token_rejected',r.latest_substage_token_rejected,
  'audit_selection_has_timestamp_ties',r.audit_selection_has_timestamp_ties,'timestamp_ties_establish_causal_order',false,
  'exact_payload_duplicate_exposure',jsonb_build_object('groups',coalesce(d.duplicate_groups,0),'excess_rows',coalesce(d.excess_rows,0),
    'rows_in_groups',coalesce(d.rows_in_groups,0),'communication_excess_rows',coalesce(d.communication_excess_rows,0),
    'non_campaign_excess_rows',coalesce(d.non_campaign_excess_rows,0),'cross_file_groups',coalesce(d.cross_file_groups,0)),
  'raw_rows_deduplicated',false
 ) quality_flags
 FROM features.customer_features f JOIN _state_rollup r USING(context_id)
 LEFT JOIN _state_duplicates d USING(context_id)
 WHERE f.feature_as_of=:'snapshot'::timestamp
)
SELECT b.context_id,b.feature_as_of,b.state_schema_version,
 -- __MODEL_JSON__
 AS state_json,b.quality_flags,
 b.sms_send_rows,
 b.sms_open_read_rows,
 b.sms_click_rows,
 b.sms_hours_since_last_send,
 b.sms_hours_since_last_engagement,
 b.sms_send_rows_7d,
 b.wa_send_rows,
 b.wa_open_read_rows,
 b.wa_click_rows,
 b.wa_hours_since_last_send,
 b.wa_hours_since_last_engagement,
 b.wa_send_rows_7d,
 b.grcs_send_rows,
 b.grcs_open_read_rows,
 b.grcs_click_rows,
 b.grcs_hours_since_last_send,
 b.grcs_hours_since_last_engagement,
 b.grcs_send_rows_7d,
 b.history_row_count,
 b.observed_history_hours,
 b.missing_channel_rows,
 b.missing_channel_send_rows_7d,
 b.send_rows_7d,
 b.distinct_application_count,
 b.distinct_product_count,
 b.hours_since_last_observed_event,
 b.latest_observed_event_name,
 b.latest_non_campaign_event_name,
 b.latest_non_campaign_journey_stage,
 b.latest_non_campaign_journey_substage,
 b.hours_since_last_non_campaign_event,
 b.observed_event_active_days_7d,
 b.aip_label_counts,
 b.aip_label_recency_hours,
 b.recent_communication_events,
 b.recent_non_campaign_journey_events,
 b.event_audit
FROM base b;
ALTER TABLE features.laya_state_v1 ADD CONSTRAINT laya_state_v1_context_pk PRIMARY KEY(context_id);
ALTER TABLE features.laya_state_v1 ALTER COLUMN feature_as_of SET NOT NULL;
ALTER TABLE features.laya_state_v1 ALTER COLUMN state_schema_version SET NOT NULL;
ALTER TABLE features.laya_state_v1 ALTER COLUMN state_json SET NOT NULL;
ALTER TABLE features.laya_state_v1 ALTER COLUMN quality_flags SET NOT NULL;
ALTER TABLE features.laya_state_v1 ADD CONSTRAINT laya_state_v1_version_check CHECK(state_schema_version='laya_state_v1');
