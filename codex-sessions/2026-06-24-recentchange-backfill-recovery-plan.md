# RecentChange Backfill Recovery Plan

Date: 2026-06-24

## Summary
- Planned a recovery approach for Wikimedia `recentchange` stream gaps after adding producer retry handling and rolling logs.
- Conclusion: missed live SSE events cannot be reliably replayed from the current Kafka/live-stream setup because Kafka only contains events that were successfully produced before the outage.
- Proposed best-effort backfill using the MediaWiki `recentchanges` Action API for the outage timeframe inferred from logs.
- No implementation was performed in this session; this log records the plan for a future implementation pass.

## Key Points
- Recovery should be treated as backfill, not exact EventStreams replay.
- The producer currently publishes raw SSE payloads to `wikimedia.recentchange.raw`, but it does not persist upstream `Last-Event-ID`, confirmed delivery checkpoints, or enough resume metadata to guarantee exact replay.
- Default recovery scope is known affected wiki domains around the outage, not all Wikimedia projects.
- The known-domain default was chosen because all-wiki recovery is much heavier and needs a domain discovery strategy.
- A user-input prompt asked for backfill scope; no answer was provided, so the plan used the recommended default: known wikis around the outage.
- The backfill path should publish to a separate topic first, not the live raw topic, because MediaWiki API records are not identical to EventStreams SSE payloads.

## Issues Encountered
- Current pipeline has no exact replay/checkpoint mechanism for events missed while the producer is disconnected or stopped.
- EventStreams data already delivered into Kafka can be consumed again from Kafka offsets, but events never produced to Kafka are outside Kafka's recovery boundary.
- MediaWiki `recentchanges` API retention may limit how far back a backfill can recover.
- Backfilled API records may differ in shape from SSE records, so downstream normalization/deduplication is required before treating them as one unified dataset.
- No API calls, Docker validation, or implementation tests were run for this plan.

## Debugging / Investigation Steps
- Inspected `event-stream/src/producer_prod.py` and confirmed the live producer writes raw JSON events directly to Kafka without storing delivery checkpoints or upstream resume IDs.
- Searched project docs and found existing notes in `docs/Kafka.md` about recovery boundaries:
  - Kafka only protects events after they have been produced to Kafka.
  - Suggested future work included tracking upstream event identity and adding a reconciliation/backfill job.
- Inspected example event payloads and schema notes showing useful identity/time fields such as:
  - `meta.dt`
  - `timestamp`
  - `rcid`
  - `meta.domain`
- Planned around MediaWiki `API:RecentChanges` parameters for time-window recovery.

## Changes Made
- None to code.
- Produced a decision-complete implementation plan in the conversation for a future standalone backfill script.

## New Knowledge / Lessons Learned
- Reconnection handling reduces future gaps but does not recover historical gaps by itself.
- The correct recovery boundary is:
  - use Kafka replay for events that made it into Kafka.
  - use API backfill for events missed before Kafka ingestion.
- A separate `wikimedia.recentchange.backfill` topic keeps recovery records auditable and avoids mixing API-shaped records directly into the SSE raw topic.
- A stable key such as `{domain}:{rcid}` should be used for backfilled records where `rcid` exists.

## Open Questions / Follow-ups
- Implement a standalone backfill script that accepts:
  - `--start` UTC timestamp.
  - `--end` UTC timestamp.
  - `--domains` comma-separated wiki domains.
  - optional `--topic`, defaulting to `wikimedia.recentchange.backfill`.
- Query each target wiki through `https://{domain}/w/api.php` with:
  - `action=query`
  - `list=recentchanges`
  - `rcstart=<start>`
  - `rcend=<end>`
  - `rcdir=newer`
  - `rclimit=max`
  - `rcprop=title|ids|sizes|flags|user|userid|comment|timestamp|loginfo|tags`
  - `rccontinue` pagination.
- Publish each recovered record with metadata:
  - `recovered=true`
  - `recovery_source="mediawiki_recentchanges_api"`
  - `recovery_window_start`
  - `recovery_window_end`
  - `source_domain`
- Add tests for query construction, pagination, Kafka key construction, and a small dry-run window.
- Decide later whether downstream consumers should merge `wikimedia.recentchange.raw` and `wikimedia.recentchange.backfill`, or whether a later normalization job should merge both into a bronze/silver schema.

## Useful References
- `event-stream/src/producer_prod.py`
- `docs/Kafka.md`
- `event-stream/examples/events-data-dict.yml`
- `event-stream/examples/events-data.json`
- MediaWiki RecentChanges API:
  - `https://www.mediawiki.org/wiki/API:RecentChanges`
- Related session logs:
  - `codex-sessions/2026-06-24-producer-network-drop-retry-fix.md`
  - `codex-sessions/2026-06-24-docker-sse-shutdown-hang.md`

---

# Plan Body: Session Log Plan: RecentChange Backfill Recovery

## Summary

- Create a new session log for the backfill recovery plan discussed after adding rolling logs and reconnection handling.
- Target filename: codex-sessions/2026-06-24-recentchange-backfill-recovery-plan.md.
- Update AGENTS.md under ## Codex Session Logs with a reverse-chronological link:
	- - [2026-06-24 — RecentChange Backfill Recovery Plan](codex-sessions/2026-06-24-recentchange-backfill-recovery-plan.md)

## Log Content

- Record that missed SSE data cannot be reliably replayed from the current Kafka/live-stream setup because Kafka only contains events successfully produced before the
outage.

- Record the planned recovery approach:
	- Use MediaWiki recentchanges API as a best-effort backfill source.
	- Infer --start and --end UTC timestamps from producer logs.
	- Query known affected wiki domains by default.
	- Publish recovered records to wikimedia.recentchange.backfill, not the live raw topic.
	- Use {domain}:{rcid} as the Kafka key where rcid exists.
	- Include recovery metadata such as recovered, recovery_source, recovery_window_start, recovery_window_end, and source_domain.

## Key Details To Preserve

- Default recovery scope: known wiki domains around the outage, because all-wiki recovery is much heavier and requires domain discovery.
- API parameters to use:
	- action=query
	- list=recentchanges
	- rcstart
	- rcend
	- rcdir=newer
	- rclimit=max
	- rcprop=title|ids|sizes|flags|user|userid|comment|timestamp|loginfo|tags
	- rccontinue for pagination.

## Verification Notes

## Assumptions

- Current Plan Mode prevents mutating repo files in this turn, so the next implementation turn should create the log and update AGENTS.md.
- Existing session logs remain unchanged.