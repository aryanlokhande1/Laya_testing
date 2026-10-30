-- Independent read-only source comparisons inside the new-table transaction.
CREATE TEMP TABLE _state_checks(name text PRIMARY KEY,passed boolean NOT NULL) ON COMMIT DROP;
INSERT INTO _state_checks VALUES
('one_row_per_eligible_context',
 (SELECT count(*) FROM features.laya_state_v1)=(SELECT count(DISTINCT context_id) FROM raw.gold_events WHERE event_datetime<:'snapshot'::timestamp AND nullif(btrim(context_id),'') IS NOT NULL)),
('expected_row_count',(SELECT count(*) FROM features.laya_state_v1)=:expected_rows),
('same_context_set_as_existing_snapshot',NOT EXISTS(
 (SELECT context_id FROM features.customer_features WHERE feature_as_of=:'snapshot'::timestamp EXCEPT SELECT context_id FROM features.laya_state_v1)
 UNION ALL(SELECT context_id FROM features.laya_state_v1 EXCEPT SELECT context_id FROM features.customer_features WHERE feature_as_of=:'snapshot'::timestamp))),
('unique_nonblank_keys',NOT EXISTS(SELECT context_id FROM features.laya_state_v1 GROUP BY 1 HAVING count(*)>1 OR nullif(btrim(context_id),'') IS NULL)),
('snapshot_and_schema_version',NOT EXISTS(SELECT 1 FROM features.laya_state_v1 WHERE feature_as_of<>:'snapshot'::timestamp OR state_schema_version<>'laya_state_v1')),
('campaign_tokens_never_become_journey_stages',NOT EXISTS(SELECT 1 FROM features.laya_state_v1 WHERE
 latest_non_campaign_event_name IN('campaign_sent','campaign_open_read','campaign_clicked') OR
 lower(btrim(latest_non_campaign_journey_stage)) IN('campaign_sent','campaign_open_read','campaign_clicked') OR
 lower(btrim(latest_non_campaign_journey_substage)) IN('campaign_sent','campaign_open_read','campaign_clicked'))),
('sequences_independently_bounded',NOT EXISTS(SELECT 1 FROM features.laya_state_v1 WHERE
 jsonb_array_length(recent_communication_events)<>least((state_json->'history'->'communication_coverage'->>'available_rows')::bigint,:communication_limit)
 OR jsonb_array_length(recent_non_campaign_journey_events)<>least((state_json->'history'->'non_campaign_coverage'->>'available_rows')::bigint,:journey_limit))),
('communication_sequence_membership',NOT EXISTS(SELECT 1 FROM features.laya_state_v1 s CROSS JOIN LATERAL jsonb_array_elements(s.recent_communication_events) e
 WHERE e->>'event_name' NOT IN('campaign_sent','campaign_open_read','campaign_clicked') OR e->>'event_name' IS NULL)),
('non_campaign_sequence_membership',NOT EXISTS(SELECT 1 FROM features.laya_state_v1 s CROSS JOIN LATERAL jsonb_array_elements(s.recent_non_campaign_journey_events) e
 WHERE nullif(btrim(e->>'event_name'),'') IS NULL OR e->>'event_name' IN('campaign_sent','campaign_open_read','campaign_clicked') OR
 lower(btrim(e->>'stage')) IN('campaign_sent','campaign_open_read','campaign_clicked') OR lower(btrim(e->>'substage')) IN('campaign_sent','campaign_open_read','campaign_clicked'))),
('no_future_audit_events',NOT EXISTS(SELECT 1 FROM features.laya_state_v1 s CROSS JOIN LATERAL jsonb_array_elements(s.event_audit->'selected_events') e
 WHERE (e->>'event_datetime')::timestamp>=s.feature_as_of)),
('positive_event_recencies',NOT EXISTS(SELECT 1 FROM features.laya_state_v1 s CROSS JOIN LATERAL jsonb_array_elements(s.recent_communication_events||s.recent_non_campaign_journey_events) e WHERE (e->>'hours_before_snapshot')::numeric<0)),
('model_sequences_match_inspection_columns',NOT EXISTS(SELECT 1 FROM features.laya_state_v1 WHERE state_json->'recent_communication_events' IS DISTINCT FROM recent_communication_events OR state_json->'recent_non_campaign_journey_events' IS DISTINCT FROM recent_non_campaign_journey_events)),
('exact_channel_codes_and_metrics',NOT EXISTS(SELECT 1 FROM features.laya_state_v1 s WHERE
 (SELECT array_agg(k ORDER BY k) FROM jsonb_object_keys(s.state_json->'channels') k)<>ARRAY['GRCS','SMS','WA']::text[]
 OR EXISTS(SELECT 1 FROM jsonb_each(s.state_json->'channels') c WHERE (SELECT array_agg(k ORDER BY k) FROM jsonb_object_keys(c.value) k)<>ARRAY['click_rows','hours_since_last_engagement','hours_since_last_send','open_read_rows','send_rows','send_rows_7d']::text[]))),
('event_channel_codes_remain_literal_or_missing',NOT EXISTS(SELECT 1 FROM features.laya_state_v1 s CROSS JOIN LATERAL jsonb_array_elements(s.recent_communication_events||s.recent_non_campaign_journey_events) e WHERE e->>'channel' NOT IN('SMS','WA','GRCS',''))),
('no_identifier_provenance_ratio_or_disclaimer_keys',NOT EXISTS(SELECT 1 FROM features.laya_state_v1 WHERE
 state_json::text ~ '"(context_id|customer_id|application_id|session_id|partner_id|product_id|source_filename|source_row_number|quality_flags|state_schema_version|affinity|matched_open_rate|unmatched_open_event_row_ratio|instructions|actions|current_application_id)"[[:space:]]*:')),
('root_model_contract',NOT EXISTS(SELECT 1 FROM features.laya_state_v1 s WHERE
 (SELECT array_agg(k ORDER BY k) FROM jsonb_object_keys(s.state_json) k)<>ARRAY['activity','aip_history','applications','channels','contact_pressure_7d','history','latest_event','latest_non_campaign_journey','products','recent_communication_events','recent_non_campaign_journey_events']::text[])),
('unavailable_contracts_not_fabricated',NOT EXISTS(SELECT 1 FROM features.laya_state_v1 WHERE
 quality_flags->'current_application_verified'<>'false'::jsonb OR quality_flags->'user_initiated_mapping_verified'<>'false'::jsonb OR
 quality_flags->'message_matching_verified'<>'false'::jsonb OR quality_flags->'response_window_complete_verified'<>'false'::jsonb OR
 quality_flags->'affinity_parameters_approved'<>'false'::jsonb OR quality_flags->'raw_rows_deduplicated'<>'false'::jsonb)),
('duplicate_exposure_totals',
 (SELECT coalesce(sum((quality_flags->'exact_payload_duplicate_exposure'->>'groups')::bigint),0) FROM features.laya_state_v1)=:duplicate_groups AND
 (SELECT coalesce(sum((quality_flags->'exact_payload_duplicate_exposure'->>'excess_rows')::bigint),0) FROM features.laya_state_v1)=:duplicate_excess_rows);

-- __REUSED_SCALAR_CHECK__

WITH events AS (
 SELECT context_id,'communication' kind,e.ordinality,(e.value->>'hours_before_snapshot')::numeric hours FROM features.laya_state_v1 s CROSS JOIN LATERAL jsonb_array_elements(s.recent_communication_events) WITH ORDINALITY e
 UNION ALL SELECT context_id,'non_campaign',e.ordinality,(e.value->>'hours_before_snapshot')::numeric FROM features.laya_state_v1 s CROSS JOIN LATERAL jsonb_array_elements(s.recent_non_campaign_journey_events) WITH ORDINALITY e
), ordered AS (SELECT *,lag(hours) OVER(PARTITION BY context_id,kind ORDER BY ordinality) previous_hours FROM events)
INSERT INTO _state_checks SELECT 'sequences_are_chronological',NOT EXISTS(SELECT 1 FROM ordered WHERE hours>previous_hours);

INSERT INTO _state_checks
SELECT 'aip_counts_and_recency_unchanged',NOT EXISTS(SELECT 1 FROM features.laya_state_v1 s JOIN features.customer_features f USING(context_id)
 WHERE s.aip_label_counts IS DISTINCT FROM f.aip_label_counts OR s.aip_label_recency_hours IS DISTINCT FROM f.aip_label_recency_hours
 OR s.state_json->'aip_history' IS DISTINCT FROM coalesce((SELECT jsonb_object_agg(a.key,jsonb_build_object('event_rows',a.value,'hours_since_last',f.aip_label_recency_hours->a.key)) FROM jsonb_each(f.aip_label_counts) a),'{}'::jsonb));

-- Oracle selects directly from raw, independently of the classification window.
WITH latest AS (
 SELECT DISTINCT ON(context_id) context_id,event_name,event_datetime,journey_stage,journey_substage
 FROM raw.gold_events WHERE event_datetime<:'snapshot'::timestamp AND nullif(btrim(context_id),'') IS NOT NULL
 AND nullif(btrim(event_name),'') IS NOT NULL AND event_name NOT IN('campaign_sent','campaign_open_read','campaign_clicked')
 ORDER BY context_id,event_datetime DESC,source_filename DESC,source_row_number DESC
)
INSERT INTO _state_checks
SELECT 'latest_non_campaign_matches_raw_oracle',NOT EXISTS(SELECT 1 FROM features.laya_state_v1 s LEFT JOIN latest l USING(context_id)
 WHERE s.latest_non_campaign_event_name IS DISTINCT FROM l.event_name
 OR s.hours_since_last_non_campaign_event IS DISTINCT FROM extract(epoch FROM (:'snapshot'::timestamp-l.event_datetime))/3600.0
 OR s.latest_non_campaign_journey_stage IS DISTINCT FROM CASE WHEN nullif(btrim(l.journey_stage),'') IS NOT NULL AND lower(btrim(l.journey_stage)) NOT IN('campaign_sent','campaign_open_read','campaign_clicked') THEN l.journey_stage END
 OR s.latest_non_campaign_journey_substage IS DISTINCT FROM CASE WHEN nullif(btrim(l.journey_substage),'') IS NOT NULL AND lower(btrim(l.journey_substage)) NOT IN('campaign_sent','campaign_open_read','campaign_clicked') THEN l.journey_substage END);

-- Every model string is allowlisted by construction. Also test all actual source
-- identifier values against the small global set of model string values. No
-- customer identifiers are printed or exported by this check.
CREATE TEMP TABLE _state_strings ON COMMIT DROP AS
 SELECT DISTINCT leaf #>> '{}' value FROM features.laya_state_v1 s
 CROSS JOIN LATERAL jsonb_path_query(s.state_json,'strict $.** ? (@.type() == "string")') leaf
 WHERE nullif(btrim(leaf #>> '{}'),'') IS NOT NULL;
CREATE UNIQUE INDEX ON _state_strings(value);
ANALYZE _state_strings;
INSERT INTO _state_checks
SELECT 'no_raw_identifier_values_in_model_state',NOT EXISTS(
 SELECT 1 FROM raw.gold_events h WHERE event_datetime<:'snapshot'::timestamp AND nullif(btrim(context_id),'') IS NOT NULL AND
 (h.context_id IN(SELECT value FROM _state_strings) OR h.application_id IN(SELECT value FROM _state_strings) OR
 h.session_id IN(SELECT value FROM _state_strings) OR h.partner_id IN(SELECT value FROM _state_strings)));

-- Ensure all selected event references identify real source rows with the same
-- context and timestamp. Physical references remain only in event_audit.
INSERT INTO _state_checks
SELECT 'event_audit_provenance_valid',NOT EXISTS(SELECT 1 FROM features.laya_state_v1 s
 CROSS JOIN LATERAL jsonb_array_elements(s.event_audit->'selected_events') e
 LEFT JOIN raw.gold_events h ON h.source_filename=e->>'source_filename' AND h.source_row_number=(e->>'source_row_number')::bigint
 WHERE h.context_id IS DISTINCT FROM s.context_id OR h.event_datetime IS DISTINCT FROM (e->>'event_datetime')::timestamp OR h.event_name IS DISTINCT FROM e->>'event_name');

SELECT json_build_object('check',name,'passed',passed) FROM _state_checks ORDER BY name;
DO $verify$
BEGIN
 IF EXISTS(SELECT 1 FROM _state_checks WHERE NOT passed) THEN
  RAISE EXCEPTION 'Laya state verification failed; new table transaction aborted';
 END IF;
END $verify$;
