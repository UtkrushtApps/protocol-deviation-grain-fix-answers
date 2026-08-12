# Solution Steps

1. Identify the grain bug: the original silver table exploded corrective_actions[] and gold was built directly from that action-level table, so one deviation with three actions became three gold fact rows.

2. Model two curated silver tables instead of one canonical flat table: silver/protocol_deviation at one row per deviation_id, and silver/protocol_deviation_action at one row per real corrective action.

3. Keep the legacy silver/protocol_deviation_flat output only as a compatibility artifact; do not use it to build the gold fact.

4. In build_silver, read bronze records, build a deviation row from parent-level fields, validate required deviation fields, and deduplicate by deviation_id.

5. In build_silver, separately iterate corrective_actions[] and emit action rows keyed by parent deviation_id plus action_id. Do not create placeholder action rows for empty arrays, so deviations with no actions still have a deviation row but zero action rows.

6. Add stable metadata and lineage columns: source_system, ingestion_ts tied to the bronze object landing time, record_hash, bronze_object_key, and bronze_record_hash.

7. Write all materialized tables with overwrite semantics and deterministic sorting, making reruns idempotent rather than append-based.

8. Build gold/fact_protocol_deviation from the deviation-grain silver table, not from the action-exploded/flat table, and defensively deduplicate by deviation_id.

9. Leave deviation_summary as a simple count over gold rows; the fix is in the model grain, not in patching the summary query.

10. Make deviation_actions read the silver action-detail table so drill-through returns all corrective actions for a deviation at action grain, ordered by action ordinal/action_id.

11. Preserve and extend validation helpers in models.py so required deviation fields are enforced consistently, and add action validation for real action-grain rows.

12. Run the pipeline and tests; IMM-07 should count four distinct deviations, PD-8831 should return CA-1/CA-2/CA-3, and repeated pipeline runs should not change fact row counts.

