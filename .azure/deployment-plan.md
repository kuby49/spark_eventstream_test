# Azure Deployment Plan: Fabric Eventstream and Spark streaming test environment

> **Revised baseline completed, failed timing, workload handed off:** Run `6c76b370-0bed-4707-b195-9e8d905fdfe4` / job `c6e7c1a2-5cb9-4055-b216-791f09b776c8`:900generated/ack/raw,90derived/Events/Scores/audits,0quarantine, generator0/no notebook error. All90timestampseach present. Pre-write Events maximum34.243844s (10over20), Scores40.720362s (5over30), so revised latency is not accepted. Raw observer exit1/132observations leaves freshness incomplete. Compact summary `.deployment\spark-prewrite-baseline-compact-summary.json`; full evaluator pending with295624character remote archive preserved. Bulk export stopped to release exclusive VM/producer/consumer/OPTIMIZE ownership to separate Streaming load limits session under user's new throughput/crash scope. No more stress work from this session, no forged baseline pass, shutdown or protection/cost change.

> **Latest user measurement override:** Measure each Events/Scores event from its carried `message_timestamp` (generator first send) to its own new nullable `spark_processed_at`, not an independent readable-row timestamp. Events marks readiness after KQL, Scores after API, both before their own output writes; Scores retains preceding Events/audit write waits. UTC and VM/Spark clock alignment are assumed. Replay preserves the first stored mark; existing historical rows are not backfilled. Reports/evaluators/gates label `spark_pre_write`, exclude output write time and explicitly do not prove the old readable-output SLA. Every-row thresholds remain20/30s. Existing four-stage durability remains unchanged; no two-commit redesign approval is inferred. No independent output observer is started for this measurement, but the separate raw publication check remains mandatory for baseline. Local127 regressions/artifact checks pass; bounded functional verification is running before any revised sustained/baseline/load.

> **Retained raw publication readback:** Private VM-token DFS/DataFusion inspection of actual RawEvents version32 matched all150 retained-run IDs, exact values and timestamps; `.deployment\retained-raw-publication-proof.json`. This is publication proof only, not historic300s freshness. The failed fault matrix's five blocked cases were caused by successful empty KQL returning the stringNaN with zero count/visibility/members. Exact-empty normalization is tested; real errors/nonempty invalid averages still quarantine. Historical failed matrix/evidence and original latency failures remain immutable. No shutdown, policy/topology/SKU/hosting change, PR or commit.

> **Corrected bounded functional matrix passed:** `fb858bee-53a9-4f5f-81fb-c6f0be69cfdb`, job `8b0cedc1-6e07-4808-a0b8-aa8ab407658a`: all7cases succeeded with synthetic processor inputs, real empty KQL/private API/Delta, actual KQL400 quarantine and explicitly injected API/Delta-write faults. Events survived API failures; explicit503quarantine replay resolved; failure after API before Scores propagated and replay returned stored random68 via actual201/200. Duplicate/metadata replay and conflict handling passed. New timestamp values exist on all5Events/3Scores. No Kafka sends or latency claim; failed prior evidence retained. Fresh actual Kafka/timestamp verification is now running.

> **Fresh changed-basis gate passed; revised baseline executing:** `03859fe6-7916-431b-9cf6-1ad239266ef0`, job `9fd8f8b5-7e42-49d9-9546-b0b62c8439cb`: Events6.000839s/Scores14.225676s, random53, one SCORED/no quarantine. Actual VM/Eventstream/Kafka event, Spark output readback, both deployed notebook cell sources verified. No independent Events/Scores observer. Local128tests/artifact checks pass. Revised900/90baseline now executes before existing gated15-minute stepped loads/OPTIMIZE; no original readable-SLA pass is implied. Old statements about redesign approval blocking all measured runs apply to the prior timing basis, not this explicit user-revised pre-write measurement.

> **DV measurement-tooling repair verified read-only:** Released Delta-rs 1.6.6 DataFusion now interprets pinned snapshots fetched through the existing private DFS VM-token route; no custom bitmap decoding, feature disable or public fallback. Atomic staged local snapshots are published only after complete materialization and successful query. Trace retains discovery/download/query/error intervals; missing/unsupported snapshots never return cached success. Real private versions215/215/413 exclude one deleted physical row each, produce logical215/215/218 and exactly match all 15 retained run rows per table plus repeated snapshots. Cold cache50/48/104s precedes arming; cached output scans~0.52s. Evidence `.deployment\protocol-reader-private-proof.json`; no fresh sends/Spark jobs or latency acceptance. Local124tests and artifact checks pass. Future measured runs remain stopped pending redesign approval; no shutdown/SKU/policy changes.

> **Prepare-only redesign selected, implementation not authorized:** See README's proposed processing redesign. Candidate combines Events with durable request/enrichment metadata and Scores with durable outcome metadata, reducing four serial success-path commits to two while retaining Events-before-HTTP and durable random API idempotency. ProcessingAudit becomes a separately checkpointed recoverable projection. Added metadata schemas, recovery-state authority and explicit audit-lag policy require approval; no SKU/topology/policy/durability relaxation is authorized or deployed. Approximately 8s of potential callback saving is an estimate, not feasibility proof.

> **Bounded OPTIMIZE/profile result and stop:** Completed run `0ac6d4c0-b495-4971-a897-66ee944e0880` compacted Events/Scores/audit from 210/210/213 files to one immediately after OPTIMIZE, proving exact unchanged logical row multisets of 215/215/218 rows. Previous file-stat totals were physical, not deletion-vector-adjusted logical counts. No Kafka sends or API calls. Warmed Events write improved from 5.790s to 4.554s/4.376s; Scores changed 3.945s/4.374s to 3.716s/4.239s; audit 4.022s/9.710s to 4.088s/4.089s. Audit version 410 is a checkpoint boundary, so its tail cannot be attributed solely to fragmentation or claimed eliminated. Approximately 16-17s for four sequential durable MERGEs plus other work does not establish required every-row margin. Post-compaction MERGEs added active deletion vectors, which the explicit independent reader cannot interpret; it must fail closed, not report visibility from deleted physical rows. No further timed run/full baseline/load starts. Preserve all failed evidence; a material design/durability/capacity decision is needed before further tuning. No implicit approval or feasibility claim; no shutdown/policy changes.

> **Latest fixed-executor result, superseding pending statements below:** Run `28369970-b046-4d5f-a414-7e8de30d14da` completes all 150 raw / 15 derived / 15 Events / 15 Scores / 15 SCORED audits with zero quarantine and all 30 independent observations (observer exit 0). Only Events/Scores latency fails: maxima **23.822761s / 36.837374s**, p50 **10.428354s / 17.524851s**. Retain the complete failed run and evaluation; no new baseline/load or acceptance claim. Actual matched job/Livy/application readback confirms dynamic allocation false, one stable executor, unchanged 16-total-core envelope, no executor removals or failed tasks; Advisor has no recommendations.

> **Remaining measured bottleneck and bounded compaction:** Worst-message callback discovery was +17.122s after first send; Events/Scores version 200 MERGE caller times were 9.109s/7.108s and checkpoint publication followed JSON publication by approximately 5s/2s. Independent output visibility still measured +23.823s/+36.837s. The observer's sole timeout was earlier in the measurement; immediately preceding worst observations had short regular reads, so no failure is waived or timestamp substituted. Active Events/Scores/audit layouts contain 210/210/213 files for only 216/216/219 rows (679KB/657KB/2.6MB). An explicit unchanged-data OPTIMIZE/profile comparison is executing with exact before/after row-multiset checks and no generator/API calls, using the existing approved layout optimization rather than changing checkpoints, durable ordering, SKU or topology. Another timed diagnostic remains gated on a concrete measured improvement. Fresh preservation reads retain Active/running, inbound Deny/outbound Allow and Function Disabled. No shutdown, PR or commit.

> **Latest sustained diagnostic completed and failed latency:** Run `36362adc-9123-4917-8e31-e0f3fdf77e9c` produced exactly 150 generated/acknowledged/raw and 15 derived/Events/Scores/SCORED audits, zero quarantine. Independent reader exit 0 covers all 15 Events and 15 Scores, with empty stderr. All correlation, KQL snapshot averages, API budgets and UTC pacing checks pass, but Events max **63.388886s** and Scores max **71.566389s** fail the unchanged targets. Preserve the complete run, `sustained-acceptance.json` and observer evidence. This supersedes older in-progress statements below; no baseline/load acceptance is claimed.

> **Read-only bottleneck investigation before more timed runs:** Real retained-consumer warm-up reached end offsets 49/40/60/40 before sends. First measured callback took 60.703s: pre-API audit write 35.019s and Scores write 11.799s, versus KQL 0.292s/API 0.416s. The worst subsequent message reached Kafka 0.561s after sending but was observed in its callback at +57.059s. Retained commitInfo metrics, Spark Advisor/stages/executors/resource usage and exact event timelines are being correlated; no engine/GC/throttling cause is presumed from wall time alone. Historical observer request/poll durations were not instrumented and cannot be reconstructed or used to waive failures. The measured key-pruning/coalesce and logical-identity fixes were included, not merely locally prepared, but did not satisfy latency. Both 900/90 baseline and stepped loads remain gated on sustained success. No shutdown, PR, commit, topology, SKU, durability, policy or threshold change.

> **Evidence-backed same-envelope session fix:** Stage 96 spent 23.070s across an actual executor decommission/replacement interval; audit360 JSON/checkpoint publication times were 04:08:20/04:08:51. Current settings/pools readback confirms the existing Medium Starter Pool, autoscale1..6 and dynamic executors1..5. Shared production/diagnostic notebook packaging now requests a fixed single executor using documented first-cell `%%configure`, with unchanged 8-core/56g driver and 8-core/56g executor (16 total cores, within the observed starting footprint). No workspace pool, SKU, core ceiling, table checkpoint or durability settings are changed. Runtime configuration and a registered single executor must be proved before measured sends. Per-request/poll observer timing sidecars preserve future timing/error gaps without replacing first-observation timestamps. Local regressions/artifact checks pass; one bounded 150/15 diagnostic is next, not a full baseline or a feasibility claim.

> **Baseline result, superseding in-progress statements:** Run `dec6911e-35a0-4ab0-9482-4b2489ae7f58` functionally produced 900 raw / 90 derived / 90 Events / 90 Scores / 90 SCORED audits, no quarantine, but **failed acceptance**. Independent reader exit 1 (`DeltaListingPaginationNotSupported`) left only 26 observed output pairs; observed max Events/Scores 70.557777/82.037949s already fails latency. Continuation listing is locally repaired/tested. Final raw-window query used reserved unquoted `first`; retained private KQL error gives `SyntaxException` and corrected aliases return 200. No retroactive average/visibility pass. Delta writes still dominate; unchanged-row comparison/layout/merge-plan diagnostics precede a bounded sustained 30/min test and any new baseline. Loads remain blocked; no shutdown or protection change.

> **Current execution, superseding pending-fresh statements below:** Random-compliant fresh run `e161e92a-d790-4c6d-b159-7b8ed82f5134` passed Events 10.272113s / Scores 18.695613s, independent observer exit 0, random score 97, one API attempt, SCORED audit and zero quarantine. The retained single KQL snapshot has average 95/count 2 with exact window members; final reported Kafka lag is zero. Read-only KQL warm-up and unchanged-row Delta warm-up are explicitly separate from measured sends. Baseline `dec6911e-35a0-4ab0-9482-4b2489ae7f58` started orchestration at `2026-11-04T01:57:10Z`; its 900/90 counts and latency acceptance remain unverified while it runs. The executing notebook/runtime snapshots are retained separately from later local changes. Local 102-test suite and artifact checks passed; prepared processor-boundary fault/replay modes have not run in cloud. No stepped load test starts before baseline acceptance. No shutdown or policy/topology/SKU changes.

> **Latest KQL correction:** Random-API fresh run `e84091be-559e-4bcb-8fda-c144a86fa8f3` failed before scoring: first count-only visibility query completed in 3.681s, next request exhausted the four-second guard; quarantine `KQLDeadlineExceeded`, zero Events/Scores, observer exit 0. No successful average was actually queried, and the original response body is unavailable. The positive-current-trigger visibility prerequisite is removed. One parameterized event-time equipment query now returns the unique average/count, trigger-presence metadata and exact snapshot members. Successful null/empty average follows the original trigger-temperature fallback; actual query errors never do. Total query budget remains four seconds/two bounded transient attempts and now checks token and response-completion elapsed time. Diagnostics warm KQL read-only, preserve snapshots, and reconcile snapshot averages separately from final raw arrival. Local 89-test suite plus artifact checks passed; a new fresh verification is running. Baseline/load tests remain gated; all failed evidence and no-shutdown/Deny-Allow/Function-Disabled constraints remain intact.

> **Latest scoring/warm-path update, 2026-11-04T01:33Z:** The original random-score contract was not satisfied by the historical rounded-temperature API. Source is corrected to standard-library random 1..100 plus read-before-conditional-create durable idempotency; old stored records are not rewritten. Private SCM deployment `9ca9b890-4f78-4d12-97e2-ce29b5a7508c` succeeded (status 4, package SHA256 `3f14cd86077cc8140bf8b8bda7dad42ad525b62830411dafafa1429b27e40340`, SCM private IP `10.74.2.9`); Function remains Disabled/B1. Private canary key `75456b44-8403-4cc5-bcc8-d86ff3231e5f:a15973e2-087b-400e-8394-0ed3c04ce42c` reused score 70 and rejected changed payload with 409. Warmed grouped fixture `75b8611b-ef68-46f0-b6e3-a5f75d4db183` passed Events 9.915s/Scores 23.514s with observer exit 0 and zero final Kafka offset lag, but predates random deployment. Warm-up re-MERGEd unchanged retained Events/Scores/audit rows; no hidden sends. The retained-scanning grouped failure 24.356s/33.773s is preserved. A random-compliant fresh gate is now running; no new baseline/load acceptance yet. All earlier deterministic scoring evidence is historical. VM/F8 stay running, workspace Deny/Allow and Function Disabled; no shutdown, PR or commit.

> **Measured baseline failure:** Run `338ea6a0-4bcd-4a8a-b68e-2be8753ff9db`: 900 generated/acknowledged/raw, 90 derived/audit, 89 Events/Scores, one `KQLDeadlineExceeded` quarantine. Independent observer exit 0; max Events/Scores latency 80.945/86.104 seconds exceeds unchanged limits. No subsequent load run is permitted yet. Root-cause fix groups Delta stage writes per microbatch, retaining Events and enrichment audit durability before per-event API calls and idempotent restart behavior; validate fresh before another baseline. A four-second bounded KQL visibility budget replaces the measured overly short 0.75-second request cap. Generator correction assigns event timestamps immediately before first send and keeps single-anchor scheduled minute/index/time separately; previous failed evidence is not rewritten or accepted. No shutdown, commit, PR, topology, SKU or protection change.

> **User-approved load extension:** After successful initial 900/90 baseline, execute 15-minute latency-only runs at 30, 60, 90, ... messages/min, retaining 10% high events/min and original 20s/30s per-record limits. Run `OPTIMIZE lhspark.Events` and `OPTIMIZE lhspark.Scores` successfully before every such run. User explicitly chose **stop at the first latency failure**. A failed/missing execution or incomplete timing coverage is not a pass and stops progression. No upper-rate/SKU/protection change or shutdown is implied. Scripts retain isolated run/checkpoint/evidence per rate and require successful initial baseline proof before starting. Initial pre-send failure `4d611f1f-eef6-4471-a026-5b712c3e9bde` produced no generator folder or manifest; preserve its evidence separately from the active guarded retry.

> **Fresh profile follow-up:** Run `49e87377-6802-4aa7-83b7-2a98aded409b` passed fresh functional correlation (Events/Scores 95, SCORED audit, one API attempt in 0.847 seconds, no quarantine). Independent reads at 18.954/28.695 seconds narrowly met the per-event targets, but a subsequent OneLake read timeout terminated the observer, so clean latency acceptance remains pending. Preserve `functional-processing-fresh-success-observer-timeout.json` and `observer-error-49e87377.json`. Bounded transient reader retries now log each error and still fail on exhaustion; verify with a fresh run before baseline. Sequential Delta merges dominate measured duration; no topology or durability relaxation is justified. The previous KQL exception was not reproduced and cannot yet be attributed to an ingestion race.

> **Current continuation:** JSON-array Kafka envelopes caused the earlier run-filter loss; production and diagnostic parsing now expand first and preserve element/transport provenance. Retained fixture processing passed exact Events/Scores/SCORED-audit correlation with zero quarantine, and independent Delta reads proved outputs plus mirrored RawEvents. Fresh run `fbce5c1e-18c4-4bae-8983-ef8df9117964` instead quarantined its event after the first KQL request failed (1.034 seconds); later KQL found the raw row. Do not infer a transport failure or accept latency from the retained success. Capture the precise safe KQL error classification, then resolve the measured cause before the 900/90 baseline. The independent reader was armed before this send but correctly observed no output rows. **Current no-shutdown/Deny-Allow/Function-Disabled instructions override all historical idle-cleanup sections below.** No new topology, resources, SKU or protection changes are authorized.

> **Latest execution override/result, 2026-11-03T21:14:42Z:** User requested repeating the VM sends after Eventstream resume, then explicitly directed "dont shut down anything" and "just run the test". Automatic shutdown was stopped and remains disabled for this test. Run `d8608b4d-b1fd-4e5d-b469-4ea586ba0c44` resumed only the existing Eventstream, observed all five required nodes Running, sent two acknowledged low/high fixtures, and proved both exact RawEvents rows in a separate private read. Evidence: `.deployment\oap-off-ingestion-gate-results.json`, `message-probe-independent-fixtures.json` and `message-probe-execution-override.json`. VM/F8/Eventstream remain running; Deny/Allow and Function Disabled are unchanged. The earlier no-message attempt stopped on observed derived Resuming before sends; diagnostic polling now handles this transition without permitting sends until the required readiness state. Current raw ingestion is proven; derived Kafka event delivery, Spark scoring/Delta and baseline/latency acceptance are still unproven. Historical status and shutdown sections below do not override the latest user instruction.

> **Latest user policy override, 2026-11-03:** User requested reverting the diagnostic to Kafka and removing workspace OAP. Full-policy GET/ETag/PUT/readback changed only outbound defaultAction to Allow, retaining inbound Deny and all existing rules. The separately hosted Function stays private-only. This supersedes historical Deny/Deny status below. A bounded Kafka diagnostic may connect to the actual endpoint's public address under this explicit authorization, using the existing VM for private notebook submission; it is not proof of the original OAP/private-Kafka acceptance. Evidence prefix: `.deployment\kafka-oap-off-`.

> **Kafka OAP-off outcome:** Job `993ff345-cd05-4eb9-9909-d5d43ad3790f` returned all diagnostic stages succeeded at `2026-11-03T17:32:30Z`: Spark readiness, destination credential identity, DNS **10.250.0.14 (private)** and **5 completed authenticated Kafka microbatches, 0 rows**. No generator/production processing or end-to-end acceptance was run. The observed bootstrap is private despite outbound Allow; this supersedes an assumption that this test necessarily uses public Kafka, but neither establishes the reason for changed DNS nor all advertised broker routes. `.deployment\kafka-oap-off-results.json` retains exact evidence. Fresh shutdown at `17:35:44Z` verifies F8 Paused/Succeeded, VM deallocated, inbound Deny/outbound Allow, Function Disabled. OAP remains intentionally disabled.

> **Lifecycle correction:** Current private topology capture found raw Eventhouse and derived stream Paused while the source/default stream/custom destination were Running. The raw destination required activation; interpretation of the extra derived-stream status is qualified below, not proof of the historical zero-row cause. Deployment, scheduled generator startup and fixture runners now share fail-closed documented `resume`/`WhenLastStopped` plus explicit all-required-node `Running` readback. Shutdown behavior is preserved; capacity resume alone is not processing resume. The collector accepts the officially documented `.platform` JSON definition part and preserves/redacts it. Mocked partial-running/resume/readback-failure regressions pass; raw/high verification remains blocked by the required readiness gate, not accepted.

> **Latest lifecycle assessment:** RawEvents destination-specific resume returned HTTP 200 at `18:12:32Z` and RawEvents reached Running. Derived stream remained Paused through `18:14:32Z`; no fixtures/baseline were sent. Official GA stream response schemas omit status, while UI guidance describes paused derived traffic as stopped and supports activation. Neither verifies pre-input semantics of the extra observed REST field, so a stopped transformation versus overstrict readiness interpretation is unresolved; retain the requested gate pending supported clarification, with no guessed derived endpoint or further live retry. `.deployment\lifecycle-status-assessment.json` preserves citations and evidence limitations. Current schema/mapping responses were captured; mirroring was explicitly omitted on redaction failure. Documented bracketed EntityName and adapted-row sanitizer defects were fixed/tested locally, not live-run. Independent read-only cleanup verification at `2026-11-03T18:21:34Z` confirms F8 Paused/Succeeded, VM deallocated, Deny/Allow and Function public access Disabled.

> **Historical status (superseded by current outbound-Allow override above):** Deployment artifacts validated and partially deployed; **acceptance unrun/false**. Workspace policy was **Deny/Deny: OAP enabled and public access denied**. Approved Spark secret/API/OneLake/Delta routes passed under OAP; private VM workspace API/OneLake passed before and after inbound Deny; an authorized outside request received `403 RequestDeniedByInboundPolicy`. Exact custom Kafka hostname was public under Allow and could not resolve under OAP. Earlier bounded diagnostic shutdown states are in `.deployment\private-source-shutdown.json`, `workspace-private-{vm,capacity}-shutdown.json` and `staged-diagnostic-shutdown.json`; resources/data are retained. MCAP CLI authorization is verified, not blocked by the unrelated corporate MCP denial.

> **API security completion:** Function `esehspark-score-api` public network access is **Disabled**, verified `2026-11-03T02:58:39Z`; public HTTPS POST received the App Service network-denial page (403). Approved PE/MPE and hostname TLS are retained. Subsequent authenticated Spark private scoring returned HTTP 201/score 83 under actual OAP, independently proving the post-cutover route.

> **Subsequent authorized diagnostic:** A separate staged notebook isolated the original pre-canary Key Vault failure as `ForbiddenByRbac`. The verified MCAP submitting user lacked secret data-plane access despite Owner; `Set-NotebookSecretAccess.ps1` added only vault-scoped Key Vault Secrets User. Live retest passed both secret reads, Spark, OneLake, private API/KV DNS, Delta and corrected ABFSS checkpoint write/readback. Production checkpoint source now normalizes the observed HTTPS endpoint to its hostname and uses the documented item-GUID path without a `.Lakehouse` suffix; this corrected definition was persisted without starting production. The bounded runner resumes only F8, preserves networking, and verifies idle suspension afterward. Latest state and outcomes are recorded separately in `.deployment\staged-diagnostic-*.json`; acceptance remains false.

> **Additional final-policy gate:** The official [workspace Private Link support matrix](https://learn.microsoft.com/fabric/security/security-workspace-level-private-links-support) excludes only Eventhouse direct ingestion in its Eventstream section but explicitly excludes consuming Eventstreams in its Eventhouse section. This contradicts preprocessing destination guidance. Original-stream to Eventhouse ingestion must be tested under final WSPL/OAP/public-deny settings; no current Allow-policy result establishes that support.

> **Final-policy source proof:** VM Kafka TLS/SASL send acknowledged one temperature-20 fixture (partition 3/offset 0), with bootstrap and all observed brokers resolving to the actual approved Fabric PE address `10.74.2.15`. Evidence `.deployment\private-source-canary.json` identifies run `da3a16a2-f57b-4357-a3ee-2bba0c0ffbc0` and message `ba54f514-2754-4617-ba29-d71757c55750`. This is not a baseline, Spark destination, or Eventhouse ingestion pass. Native-stderr and process-exit capture bootstrap defects were corrected and tested; separate redacted VM log verification confirmed successful dependency installation/import and zero active generator/canary/installer processes.

> **Current evidence:** `.deployment\final-preservation-summary.json` and `final-{workspace-policy,capacity,vm,function,resource-inventory,private-endpoints}.json` are fresh read-only preservation snapshots. Final-policy Eventhouse readback is recorded separately in `private-eventhouse-canary.json`; metadata uses the real GA item details/display name, not the nonexistent `properties.databaseName` field. No unapproved private hostname, DNS override or public query fallback is used.

> **Final-policy Eventhouse result:** Corrected Kusto v1 result-shape handling enabled authenticated private queries at `10.74.2.11`. The three-minute readback window ending `2026-11-03T04:48:09Z` found **0 matching rows** for the acknowledged source fixture. Thus VM private KQL is proven, while original-stream ingestion is not; no exact policy attribution or general unsupported-topology conclusion is made from an empty result. This does not prove Spark-private KQL. OneLake mirroring/shortcut configuration is verified, but actual publication/read of this fixture is not observed. No more baseline/compute retries are justified while the exact Spark Kafka route remains blocked.

> **Final preserve-only state, 2026-11-03T04:51:28Z:** Independent fresh ARM, VM and full-policy reads confirm F8 **Paused/provisioning Succeeded**, VM **PowerState/deallocated**, workspace **Deny/Deny** and Function public access **Disabled**. All resources/data are retained. `.deployment\final-data-gates.json` is the final exact-fixture/gate/evidence catalog. Baseline remains unrun and acceptance false; no commit or PR was created.

Generated: 2026-11-02

**User-approved Event Hubs/AMQP alternative tested, 2026-11-03T17:08Z:** The user approved using the existing Custom endpoint Event Hubs interface, plus temporary F8/VM activation through the verified private workspace route. Separate diagnostic notebook `ed99b3aa-98cd-43ae-bc63-d2e88c70aa4a`/job `1e5f80c8-89ad-4ddc-b9a2-082e338fc918` returned structured prerequisites: Spark readiness passed on `4.1.1.5.5.20260929.238183312`; native Event Hubs connector provider class was absent (`ClassNotFoundException`); destination DNS independently failed (`gaierror`) under Deny/Deny. No AMQP connection/secret retrieval, connector installation, production notebook edit, preview destination or topology change occurred. Job Completed means the diagnostic returned its failed-stage evidence, not ingestion success. `.deployment\eventhubs-amqp-results.json` retains results. `.deployment\eventhubs-amqp-shutdown.json` independently verifies F8 Paused/Succeeded, VM deallocated, Deny/Deny and Function Disabled at `2026-11-03T17:13:24Z`. Switching protocol did not establish the missing private route; acceptance remains false.

**Bounded current read-only capture, 2026-11-03T05:10:13Z:** MCAP context was verified and preservation reads passed before/after. Local workspace-private API DNS returned public `20.150.160.125` instead of approved `10.74.2.10`. The authorized management `GET .../eventstreams/{id}/topology` returned 403 `RequestDeniedByInboundPolicy`; `Read-CurrentFabricIngestionEvidence.ps1` recorded `.deployment\current-ingestion-capture-result.json` and stopped. No definition/state/mapping capture, public KQL call, mutation or compute resume occurred. Current and historical destination/mapping evidence remains unavailable from this idle execution host; this result is not an ingestion fault diagnosis. F8 Paused, VM deallocated, Deny/Deny and Function Disabled were freshly verified again afterward.

**Local evidence collectors updated; not live-run:** `scripts\FabricEvidence.ps1` shares decoded-JSON credential sanitization and ignored evidence persistence. Eventstream readback now saves full definition parts, the [GA topology response](https://learn.microsoft.com/en-us/rest/api/fabric/eventstream/topology/get-eventstream-topology) and available component status, and checks graph edges/raw destination identity/filter/serialization. A failed documented API is explicit; absent required status fails with `-RequireComponentState`, never via guessed endpoints. Eventhouse readback retains full schema/mirroring and [table/database JSON ingestion mappings](https://learn.microsoft.com/en-us/kusto/management/show-ingestion-mapping-command?view=microsoft-fabric) with query context. Local redaction/graph/mapping/state regression checks cover the change. These collectors cannot reconstruct historical fixture state and require a future authorized private live run; no deployment/compute/protection change occurred. Supported Spark-private Kafka routing and overall acceptance remain unresolved.

## 1. Project Overview

**Goal:** Build, deploy, privately validate, accept, and safely idle a reproducible Fabric Eventstream/Spark streaming system.

**Path:** New Project (empty repository)

The first approved Azure deployment attempt partially provisioned the resource group; see the deployment and validation evidence in Section 7. This file records the coordinator-provided, user-approved contract; implementation is restricted to this isolated worktree.

## 2. Requirements

| Attribute | Value |
|-----------|-------|
| Classification | Development / test |
| Scale | Small; 30 events/min for a 30-minute acceptance run |
| Budget | Fixed SKUs, cost-visible, suspend/deallocate after test |
| Subscription | ME-MngEnvMCAP885977-eruskal-1 (`0a0cf761-9d5d-4862-9165-7b69c188a0ee`) |
| Location | West US 3 (`westus3`) |

Subscription and region were selected and approved in the parent session; no tenant re-selection is required.

## 3. Components Detected

| Component | Type | Technology | Path |
|-----------|------|------------|------|
| VM generator | Producer | Python, Kafka/SAS producer | `src/generator/` |
| Eventstream | Streaming router | Microsoft Fabric Eventstream | Fabric workspace |
| Eventhouse | Raw event store | Microsoft Fabric Eventhouse/KQL | Fabric workspace |
| Spark pipeline | Streaming worker | Fabric Notebook, PySpark, Kafka Structured Streaming | `fabric/notebooks/` |
| Lakehouse | Delta outputs/shortcut | Microsoft Fabric Lakehouse | Fabric workspace |
| Scoring API | HTTP API | Python Azure Functions v4 on Linux App Service B1 | `src/scoring_api/` |
| API idempotency store | Persistent state | Azure Storage Tables, managed identity | Azure resource group |
| Network/security | Private network | VNet, subnets, NSGs, NAT Gateway, Bastion, private endpoints/DNS | `infra/` |
| Secrets | Secrets management | Azure Key Vault, RBAC, managed identity | Azure resource group |
| Orchestration | Infrastructure and Fabric control | PowerShell + ARM/Bicep + Fabric REST | `scripts/`, `infra/` |

## 4. Recipe Selection

**Selected:** Bicep + PowerShell (Azure CLI for deployment and Fabric REST for Fabric SaaS items)

**Rationale:** The user approved PowerShell orchestration and a Bicep/template-backed infrastructure repository. AZD is not selected because the deployment includes non-AZD Fabric control-plane operations and explicitly requires a custom staged network cutover. Azure Functions HTTP source is based on the discovered official Python HTTP Functions template; its MCP retrieval returned HTTP 302, so the template's documented official Azure-Samples GitHub fallback is being used. The template's Flex Consumption infrastructure conflicts with the user's explicit dedicated Linux App Service B1 choice; it will not be deployed or silently substituted.

## 5. Architecture

**Stack:** Azure infrastructure + Fabric SaaS; API on dedicated Linux App Service B1

### Service Mapping

| Component | Azure/Fabric Service | SKU |
|-----------|----------------------|-----|
| Fabric streaming capacity | Microsoft Fabric capacity | F8 (8 CU), West US 3 |
| Workspace | Fabric workspace | assigned to `fabricessparktest8` |
| Eventstream, Eventhouse, Lakehouse, Notebook | Fabric items | capacity-bound |
| Generator VM | Windows VM | `Standard_D2s_v5` (2 vCPU), Windows Server 2022 |
| VM remote access | Azure Bastion | Basic |
| Outbound VM egress | Azure NAT Gateway | Standard |
| Scoring API plan | Linux App Service plan | B1, one instance, Always On |
| API/runtime/idempotency | Azure Storage Tables + Function host storage | Standard LRS; managed identity |
| Secrets | Azure Key Vault | Standard, RBAC, soft delete/purge protection |
| Fabric private access | Fabric workspace-level Private Link | global PLS + private endpoint in VNet |
| API/KV/storage access | Private Link endpoints | private DNS |

### Supporting Services

| Service | Purpose |
|---------|---------|
| Azure Monitor / Application Insights | Function diagnostics and dependencies |
| Key Vault | Eventstream credentials and Function-specific API key |
| Managed identities | Function host/runtime storage and API storage access |
| Private DNS zones | Resolve workspace, KV, API, and storage private endpoints |

## 6. Provisioning Limit Checklist

### Phase 1: Resource inventory

| Resource Type | Number to Deploy | Total After Deployment | Limit/Quota | Notes |
|---------------|-----------------|------------------------|-------------|-------|
| `Microsoft.Compute/virtualMachines` (`Standard_D2s_v5`) | 1 | At least 1 additional VM; 2 vCPU | 100 regional cores; DSv5 family 100 vCPU | CLI quota list; actual current regional usage checked below |
| `Microsoft.Network/publicIPAddresses` (Bastion + NAT) | 2 | 9 if current usage remains 7 | 1,000 public IPs | CLI quota usage |
| `Microsoft.Network/privateEndpoints` | 8 planned (Fabric, API sites/SCM, KV, storage subresources) | 9 if current usage remains 1 | 65,536 private endpoints | CLI quota usage; exact count follows supported topology |
| `Microsoft.Storage/storageAccounts` | 1 (Function host + API Table store) | 6 if current usage remains 5 | 250 accounts | CLI quota usage |
| `Microsoft.Fabric/capacities` | 1 x F8 | 32 CU if current usage remains 24 | 512 CU | Fabric Capacities List Usages API |
| `Microsoft.Web/serverfarms` (Linux B1) | 1 | 1 additional B1 plan | West US 3 B1 listed | `az appservice list-locations` |
| `Microsoft.KeyVault/vaults` | 1 | 1 additional vault | Name availability + subscription policy checked | `az keyvault check-name` |

### Phase 2: Quota and capacity evidence

Checked 2026-11-02 against the selected subscription and `westus3`.

| Resource/quota | Current usage | Limit | Planned additional | Result/source |
|----------------|----------------|-------|--------------------|---------------|
| Regional cores | 16 | 100 vCPU | 2 vCPU | Pass; `az quota usage show cores` + `az quota list` |
| Standard DSv5 family | 0 | 100 vCPU | 2 vCPU | Pass; `az quota usage show standardDSv5Family` + `az quota list` |
| Public IPs | 7 | 1,000 | 2 | Pass; Network quota CLI |
| Private endpoints | 1 | 65,536 | 8 estimated | Pass; Network quota CLI |
| Storage accounts | 5 | 250 | 1 | Pass; Storage quota CLI |
| Fabric capacity units | 24 | 512 CU | 8 CU | Pass; `Microsoft.Fabric/locations/westus3/usages?api-version=2023-11-01` |
| Linux B1 plan in West US 3 | Not a counted CU quota | Supported region | 1 plan | Region listed by `az appservice list-locations --sku B1 --linux-workers-enabled` |
| Standard_D2s_v5 in West US 3 | SKU unrestricted | N/A | 1 VM | `az vm list-skus` returned no restrictions |

Microsoft.Fabric is quota-checked via the documented ARM usage API; the generic `az quota` CLI does not expose Fabric capacity quota. Network quota values are verified against actual usage endpoints; `isQuotaApplicable=false` is recorded for the Network quota usage records.

### Policy constraints

The subscription policy assignments returned by `az policy assignment list` are Defender/ASC defaults only; no explicit resource type, SKU, region, tag, or public network policies were reported in the subscription-level results. Required providers are registered: Microsoft.Compute, Microsoft.Network, Microsoft.Storage, Microsoft.KeyVault, Microsoft.Web, Microsoft.Insights, Microsoft.OperationalInsights, Microsoft.Fabric, Microsoft.Quota.

### Fabric tenant and fixed-name checks

Verified 2026-11-02 in the MCAP tenant:

| Check | Result | Evidence |
|---|---|---|
| Fabric tenant home region | West US 3 (Arizona) | Fabric portal Help and Support > About Fabric; `Your home region is West US 3 (Arizona)` |
| Fabric Data Engineering region prerequisite | West US 3 is the selected capacity region; both the home/capacity regions must support Data Engineering for managed private endpoints | [Fabric region availability](https://learn.microsoft.com/fabric/admin/region-availability); recheck in validation |
| Workspace-level inbound network rules tenant setting | Enabled | `GET /v1/admin/tenantsettings`, `WorkspaceBlockInboundAccess=true` |
| Workspace-level outbound network rules tenant setting | Enabled | `GET /v1/admin/tenantsettings`, `WorkspaceBlockOutboundAccess=true` |
| Tenant-level Private Link | Disabled | `AllowAccessOverPrivateLinks=false`; not required for workspace-level Private Link |
| Tenant-wide block of public access | Disabled | `BlockAccessFromPublicNetworks=false`; keep unchanged because only the target workspace is to be restricted |
| Fixed Fabric capacity name | No matching item found | Fabric capacity list returned 9 capacities; none named `fabricessparktest8` |
| Fixed Fabric workspace name | No matching item found | Fabric workspace list returned 61 workspaces; none named `ES_EH_Spark_test` |
| Azure resource group at preflight | Not present | `az group exists --name rg_esehspark` returned `false` before deployment |
| Fixed API host name | Available at preflight | Azure App Service name availability check for `esehspark-score-api` |
| ARM control-plane token | Azure CLI context worked for read-only preflight; a real ARM deployment write was authorized | Deployment result is recorded in Section 7; do not infer private/data-plane access from ARM authorization |

The Fabric portal confirmation followed Microsoft's documented About-pane procedure. Do not infer tenant home region from capacity regions or Entra tenant metadata.

### Cost preflight (USD, Consumption retail, West US 3 where regional)

| Component | Meter | Rate | Source/qualification |
|-----------|-------|------|----------------------|
| Fabric F8 | Fabric Capacity / Compute Pool Capacity Usage CU | $0.18/CU-hour = $1.44/hour at 8 CU | Retail Prices API; query all Microsoft Fabric service rows, not F8 ARM-SKU filter |
| Windows VM | Standard_D2s_v5 Windows | $0.188/hour | Retail Prices API `westus3` |
| Bastion | Basic Gateway | $0.19/hour | Retail Prices API `westus3` |
| Linux B1 plan | App Service Basic Plan | $0.017/hour | Retail Prices API evidence |
| Public IPs | Standard IPv4 Static | $0.005/hour each | Retail Prices API evidence |
| NAT Gateway | Standard Gateway + Standard Data Processed | $0.045/hour + $0.045/GB | Global meter; both outbound and return traffic |
| Private endpoints | Standard Private Endpoint | $0.01/endpoint-hour + data processing | Global meter |
| Disks, storage, Key Vault, transfer, data processing | Usage-dependent | Must be itemized from exact deployed SKU/size/usage | No zero-cost assumption |

Refreshed through Azure Retail Pricing MCP on 2026-11-02: Fabric's actual meter is `Compute Pool Capacity Usage CU` at $0.18/CU-hour (F8 = $1.44/hour); Windows D2s_v5 $0.188/hour; Bastion Basic Gateway $0.19/hour; Linux B1 $0.017/hour; NAT Standard Gateway $0.045/hour and data processed $0.045/GB; Standard Private Endpoint $0.01/endpoint-hour. The public IP meter is $0.005/hour each. The partial subtotal is $1.44 + $0.188 + $0.19 + $0.017 + two public IPs ($0.01) + NAT ($0.045) + `N` PEs ($0.01 × N) = **$1.89 + $0.01×N per active hour**, before VM disks, storage, Key Vault, private-link data processing, NAT data, transfer, and other usage. Recalculate with exact deployed endpoint and IP counts; F8 is suspended and VM deallocated after acceptance. App Service B1 plan, Bastion, NAT, PEs, disks, IPs, storage and KV continue billing while retained.

## 7. Execution Checklist

### Phase 1: Planning
- [x] Analyze workspace (empty apart from Git metadata)
- [x] Gather requirements from approved parent plan
- [x] Confirm subscription and location (approved parent context)
- [x] Resource inventory and quota validation
- [x] Scan codebase (empty)
- [x] Select Bicep + PowerShell recipe
- [x] Plan architecture and preserve approved topology
- [x] User approved this plan in the parent session

### Phase 2: Execution
- [x] Research components and validate API schemas
- [x] Generate infrastructure and application artifacts
- [x] Apply private network and secret hardening
- [x] Run local functional tests
- [x] Update status to Ready for Validation

### Phase 3: Validation
- [x] Invoke azure-validate
- [x] All validation checks pass
- [x] Record validation proof
- [x] Update status to Validated

#### All validation checks pass
- [x] Core Azure CLI validation: account context, Bicep build, subscription-scope template validation, and what-if
- [x] Bicep linter
- [x] Python compile and unit tests
- [x] PowerShell parser and deployment-artifact checks
- [x] Static managed-identity/RBAC review against application data-plane operations
- [x] Subscription policy assignments reviewed; no conflicting policy blocks the approved fixed SKUs or network design

### Phase 4: Deployment
- [x] Invoke azure-deploy (explicitly requested); first ARM attempt partially provisioned resources
- [x] Deploy approved Azure infrastructure/capacity and provision the Fabric workspace, requested items, Eventhouse schema, Eventstream graph, and Lakehouse shortcut
- [x] Deploy the scoring Function on the approved B1 plan; VM-side private DNS/TLS/API/Key Vault canary passed
- [x] Create and approve Spark managed private endpoints for the Function and Key Vault; deploy the production notebook definition
- [ ] Diagnose the failed Spark-only private-route canary before rerunning it
- [ ] Discover and prove a supported real Spark-managed Kafka target; do not invent an Event Hubs resource ID
- [ ] Prove generator-to-Eventstream ingestion and all private data paths before OAP/public deny
- [ ] Run acceptance and fault fixtures
- [ ] Capture evidence, drain, suspend exact F8 and deallocate VM
- [x] User-authorized alternative for zero-input/no-active-work state: export diagnostics and full policy, verify no producer/job, suspend exact F8, deallocate exact VM, verify both states; acceptance remains false

## 7. Validation Proof

**Current code-only update status: Validated (azure-validate, 2026-11-04T02:25+01:00).** No infrastructure, policy, identity, SKU or application settings change is included. The original subscription/West US 3 approval remains in force. The private ZIP deployment must preserve Function public access Disabled and B1; it transports an Entra deployment token only as CMS ciphertext encrypted to an ephemeral non-exportable VM certificate, never as plaintext script content. Existing Table Data Contributor permits the new read-before-conditional-create path; no RBAC grant is needed. Historical ARM validation/what-if remains applicable to unchanged templates; no infrastructure deployment will run.

| Code-only check | Actual result |
|---|---|
| `python -m unittest discover -s tests -p 'test_*.py'` | 79 tests passed, including controlled random range, different valid outcomes, repeated request reuse and concurrent conditional-insert convergence |
| `.\tests\Test-DeploymentArtifacts.ps1` | PowerShell syntax, notebook package, graph and KQL checks passed |
| `python -m py_compile src\scoring_api\logic.py src\scoring_api\storage.py src\scoring_api\function_app.py`; `git diff --check` | Passed |
| Isolated `az account show`, `az functionapp show`, `az appservice plan show` | Approved MCAP tenant/subscription; exact Function Running/public access Disabled; exact existing Linux B1 plan |
| Static `infra\main.bicep` / storage adapter review | Existing Function storage-scoped Table Data Contributor covers reads and conditional inserts; no new roles/resources/configuration |

> The azure-validate skill must populate this section with actual commands, results, and timestamps before status becomes `Validated`.

| Check | Command Run | Result | Timestamp |
|-------|-------------|--------|-----------|
| Isolated MCAP preflight | `AZURE_CONFIG_DIR=$HOME\.azure-mcap; ./scripts/Invoke-Preflight.ps1` | Correct tenant/subscription; providers registered; D2s_v5 unrestricted; 16/100 regional and 0/100 DSv5 vCPU; fixed names available; resource group absent | 2026-11-03 |
| Subscription policies | `az policy assignment list --scope /subscriptions/0a0cf761-9d5d-4862-9165-7b69c188a0ee` | Defender/ASC assignments only; no conflicting SKU, region, or network policy found | 2026-11-03 |
| ARM core validation | `AZURE_CONFIG_DIR=$HOME\.azure-mcap; ./scripts/Validate-Deployment.ps1` | PASS after capacity administrator input was corrected to UPN; authenticated; Bicep compiled; subscription-scope validation and what-if passed with 51 planned changes and 0 deletes. | 2026-11-03T01:20+01:00 |
| Bicep builds and lint | `az bicep build` and `az bicep lint` for `entrypoint.bicep` and `main.bicep` | Both changed templates compiled and linted successfully; generated ARM JSON refreshed. | 2026-11-03T01:20+01:00 |
| Fabric workspace private-link template | `az bicep build --file .\infra\fabric-link.bicep`; `az deployment group what-if ...` | PASS; five expected resources to create, 38 existing resources ignored, zero deletes. | 2026-11-03 |
| Python checks | `python -m pytest -q` | 18 tests passed; 4 subtests passed | 2026-11-03T01:20+01:00 |
| PowerShell/artifact checks | `./tests/Test-DeploymentArtifacts.ps1` | All scripts parsed; notebook package, Eventstream graph, KQL and lifecycle gates passed | 2026-11-03 |
| Corrected Function deployment invariants and regression suite | `./tests/Test-DeploymentArtifacts.ps1`; `python -m unittest discover -s tests -v`; `python -m py_compile src\generator\producer.py src\scoring_api\logic.py src\scoring_api\storage.py src\scoring_api\function_app.py fabric\notebooks\streaming_pipeline.py`; `git diff --check` | PASS; PowerShell/artifact checks passed, all 18 Python tests passed, all listed Python files compiled, and no whitespace errors. The test asserts remote Oryx build, VNet route-all, and removal of `WEBSITE_RUN_FROM_PACKAGE`. | 2026-11-03T02:38+01:00 |
| Latest Bicep validation | `./scripts/Validate-Deployment.ps1`; `az bicep lint --file .\infra\main.bicep`; `az bicep lint --file .\infra\fabric-link.bicep` | PASS; approved tenant/subscription, build, Azure template validation, and subscription what-if passed with 57 planned changes and 0 deletes; both Bicep lints exited successfully. Validation ran without a resource write. | 2026-11-03T02:38+01:00 |
| Streaming authoring changes | `python -m pytest -q`; `./tests/Test-DeploymentArtifacts.ps1`; `git diff --check` | PASS; 18 tests and 4 subtests; PowerShell syntax, notebook package, Eventstream graph, KQL schema, and whitespace checks passed. | 2026-11-03T00:39:38Z |
| Bounded private-route documentation review | Microsoft Learn pages: Eventstream network-feature guide; tenant/workspace Private Link support table; Eventstream MPE guide; Fabric MPE creation guide | Conflicting support statements confirmed: one page says Custom Endpoint source/destination are supported with Private Links, while the detailed table says source Yes/destination No. Eventstream MPE docs cover Azure Event Hubs/IoT Hub sources and require an Azure target resource ID. No private Spark Kafka route demonstrated; no topology change made. | 2026-11-03 |
| Idle-shutdown evidence gate and local regression | `python -m pytest tests -q`; `pwsh -NoProfile -File .\tests\Test-DeploymentArtifacts.ps1`; `python -m py_compile ...`; incomplete-evidence invocation of `Stop-TestEnvironment.ps1 -IdleShutdownEvidencePath` | PASS; 18 Python tests, 4 subtests, PowerShell artifact checks and Python compilation passed. The idle path rejects incomplete evidence before making an Azure call. The then-reported MCP blocker was subsequently corrected as a wrong-identity diagnosis; it did not establish a MCAP denial. | 2026-11-03 |
| Historical wrong-identity access recheck | Azure MCP `azure-group_resource_list` for `rg_esehspark` in subscription `0a0cf761-9d5d-4862-9165-7b69c188a0ee` | Corporate identity returned HTTP 403 `AuthorizationFailed` for `Microsoft.Resources/subscriptions/read`; not evidence against approved MCAP authorization. Superseded by verified isolated MCAP CLI access below. | 2026-11-03 |
| Correct MCAP identity and access | `AZURE_CONFIG_DIR=$HOME\.azure-mcap`; `az account show --subscription 0a0cf761-9d5d-4862-9165-7b69c188a0ee`; `az resource list -g rg_esehspark --subscription ...` | PASS; approved tenant/subscription and MCAP lab administrator; resource inventory succeeded. The corporate MCP403 was an identity mismatch, not a MCAP permission denial. | 2026-11-03 |
| Fresh zero-workload evidence | `.\scripts\Export-IdleShutdownEvidence.ps1` | PASS; persisted resource and PE inventory, complete workspace policy (Allow/Allow), terminal notebook jobs, failed Livy session, and VM task/process/DNS diagnostics. No generator tasks/processes; no active notebook jobs; no baseline events to drain or mirror. Acceptance explicitly false. | 2026-11-03 |
| Approved preserve-only idle shutdown | `.\scripts\Stop-TestEnvironment.ps1 -IdleShutdownEvidencePath .\.deployment\idle-shutdown-evidence.json -Confirm:$false`; independent ARM capacity and VM instance-view reads | PASS; exact F8 Paused/provisioning Succeeded; exact VM PowerState/deallocated. Result persisted in `.deployment\shutdown-result.json`. All resources/data retained; no network-policy changes. | 2026-11-03T02:55:13Z |
| Historical pre-cutover service network state | `az functionapp show`; `az keyvault show`; `az storage account show`, each with isolated MCAP config and explicit subscription | At this read Function public network was Enabled; Key Vault/storage were Disabled/default Deny. Function Enabled is superseded by the verified private-only cutover below, not the current state. | 2026-11-03 |
| Function private-only cutover | `.\scripts\Set-ScoringFunctionPrivateOnly.ps1` | PASS; saved authenticated VM private canary verified as prerequisite; exact Function publicNetworkAccess Disabled; public HTTPS POST rejected by App Service network restriction page, HTTP403. Approved sites PE retained. F8 Paused and VM deallocated independently rechecked; no compute resumed. Evidence: `.deployment\function-private-only-cutover.json`. | 2026-11-03T02:58:39Z |
| Private-only redeployment protection | `az bicep build --file .\infra\entrypoint.bicep`; `.\tests\Test-DeploymentArtifacts.ps1`; `python -m pytest tests -q` | PASS; subscription template compiles; redeployment script reads/preserves existing Function public-network setting and passes it through the template. PowerShell artifact checks and 18 Python tests/4 subtests pass. No infrastructure redeployment performed. | 2026-11-03 |
| Offline failed-notebook inspection | Python AST parse/compile plus persisted ipynb, job and Livy evidence inspection | Syntax valid, correct Lakehouse binding, one code cell/zero outputs, no failed-statement detail or resource URI. Runtime initialization precedes canary checks. No specific source fault established; no speculative source change or compute restart. Evidence: `.deployment\notebook-offline-diagnosis.json`. | 2026-11-03 |
| Lakehouse raw shortcut | `./scripts/Set-LakehouseRawShortcut.ps1` | PASS; existing `RawEvents` shortcut target read back correctly after normalizing the API's `/Tables` path. | 2026-11-03 |
| Static identity/RBAC review | Reviewed role assignments in `infra/main.bicep` against `src/generator/producer.py`, `src/scoring_api/storage.py`, and Function host storage settings | VM identity has Key Vault Secrets User; Function identity has Table Data Contributor, Blob Data Owner, and Queue Data Contributor on its storage account. Fabric notebook Key Vault access remains a runtime connection/MPE proof gate. | 2026-11-03 |

**Validated by:** azure-validate.
**Validation timestamp:** 2026-11-03T02:38+01:00.

Validation confirms the corrected templates and local artifacts. The initial ARM attempt failed capacity-administrator validation because it supplied the Entra object ID; the template was corrected to pass the signed-in user's UPN and the in-place retry succeeded. At that deployment verification, F8 was active. `rg_esehspark` contains the VM, B1 Function, Key Vault, storage and private endpoints. The workspace is assigned to the capacity; `ehspark`, `lhspark`, `esehnotebookspark`, and `esspark` exist. Schema/graph/shortcut configuration is verified, but this is not final-policy ingestion acceptance. Function/API and KV MPEs are Succeeded/Approved; subsequent bounded diagnostics and latest idle states supersede the initial compute state.

A bounded Spark-only private-route canary failed in job `6c6d86b8-a755-4473-9629-3a5c28e1d6d6` (Livy session `b151a886-abe7-4cae-b68f-41faf07e8b43`, Spark application `application_1793672561955_0001`) with generic `System_Cancelled_Session_Statements_Failed`. Spark Advisor returned no findings; Resource Usage reports `capacityExceeded=false`, no Spark tasks, and zero core efficiency. That historical response did not identify the exact failed statement. Subsequent staged diagnostics established and corrected Key Vault `ForbiddenByRbac` and the malformed ABFSS checkpoint, then passed the production prerequisites under OAP; those live results supersede the earlier undiagnosed-prerequisite status. The driver-stderr request did not return logs because the Windows CLI command split the query parameters; the Livy log established application startup but did not reveal a root cause. The notebook snapshot is [available in Fabric](https://app.powerbi.com/workloads/de-ds/sparkmonitor/e9dbc832-0b78-4a4f-ba72-d6474289c6d4/b151a886-abe7-4cae-b68f-41faf07e8b43?trident=1&experience=power-bi&ctid=4526cee1-4399-4e4b-9134-46fabd583527&tab=related) and requires an authenticated MCAP Power BI session for cell-level details. The corrected streaming definition is persisted but not running; the unresolved exact Kafka private route still prevents a production retry.

The bounded official-document review is preserved in `.deployment/private-route-doc-review.json`. Microsoft Learn's [Eventstream network-feature guide](https://learn.microsoft.com/fabric/real-time-intelligence/event-streams/choose-the-right-network-security-feature) says custom endpoint sources and destinations are supported with Private Links, but the explicit [tenant/workspace Private Link support table](https://learn.microsoft.com/fabric/real-time-intelligence/event-streams/set-up-tenant-workspace-private-links#supported-scenarios) lists Custom Endpoint source as **Yes** and Custom Endpoint destination as **No**. The guide classifies a custom endpoint destination as an external application pulling events (inbound); it does not establish a private Spark Kafka consumer path. The [Eventstream managed private endpoint guide](https://learn.microsoft.com/fabric/real-time-intelligence/event-streams/set-up-private-endpoint) limits its MPE support to Azure Event Hubs and IoT Hub sources and requires an Azure resource ID. The [Fabric MPE creation guide](https://learn.microsoft.com/fabric/security/security-managed-private-endpoints-create) likewise requires a target Azure resource ID and says an FQDN alone is unsupported. The actual Eventstream destination metadata contains no supported Azure target resource ID. Therefore the documented private Kafka path is not demonstrated and remains an unresolved hard gate; this review does not claim that every possible same-workspace route is impossible. No topology change, public fallback, OAP enablement, or public-access denial was made.

At the first idle shutdown, no generator canary or acceptance run had occurred and policy was Allow/Allow. Those historical snapshots are superseded by the subsequent Deny/Deny cutover and private/public proofs at the top of this document. Acceptance and exact private Kafka consumption remain incomplete. The corporate MCP 403 used the wrong identity; isolated MCAP CLI successfully verified the approved administrator and performed the authorized work without any corporate grant.

The documented [Fabric cross-workspace MPE mechanism](https://learn.microsoft.com/fabric/security/workspace-outbound-access-protection-allow-list-endpoint) accepts the actual `Microsoft.Fabric/privateLinkServicesForFabric` resource ID with subresource `workspace`; `pl-fabric-esehspark` is an existing real target of this type. This corrects the overly broad inference that every possible route requires a customer-owned Event Hubs ID. The mechanism is documented for another target workspace; it does not demonstrate same-workspace loopback routing of the custom Kafka hostname or its advertised brokers. No speculative self-targeted MPE was created; the exact route remains unproven.

The initial `idle-*.json` export captured no generator task/process, private source DNS `10.74.2.15`, terminal production jobs and the failed Livy session without a resource URI. `.deployment\shutdown-result.json` records the first Paused/deallocated state at `2026-11-03T02:55:13Z`, with acceptance false and no baseline data. Later diagnostic records supersede those initial snapshots: Function public access is Disabled; workspace policy is Deny/Deny; private API/OneLake and an authorized public-negative test passed. All resources/data are retained. Latest source fixture and idle proof are recorded separately; no historical Allow or Enabled observation is presented as current.

## 8. Files to Generate

| File | Purpose | Status |
|------|---------|--------|
| `.azure/deployment-plan.md` | Approved architecture, quotas, workflow and proof | Validated; execution status current |
| `.deployment/private-route-doc-review.json` | Historical redacted official-doc findings | Retained; superseded for current state by staged diagnostics and final policy/idle evidence |
| `infra/` | Versioned Azure templates and parameters | Implemented, validated, and deployed |
| `scripts/` | PowerShell preflight/deploy/Fabric/network/start/stop automation | Implemented and locally validated |
| `src/generator/` | Exact-rate source producer | Implemented and unit-tested; deployed canary pending |
| `src/scoring_api/` | HTTP scoring API and tests | Implemented, unit-tested, and deployed |
| `fabric/` | Eventstream, notebook, environment, KQL and table definitions | Implemented; notebook deployed; data-path proof incomplete |
| `README.md` | Deployment, operations, cost and support runbook | Updated with current blockers and lifecycle state |

## 9. Current Blockers and Safe Continuation

The fixed-name resources and source artifacts are in place, but the approved data path has not passed end-to-end acceptance. Do not start a baseline producer/stream or claim the exact design is complete. The original pre-canary fault was isolated as Key Vault `ForbiddenByRbac`, fixed with verified MCAP submitter secret access and retested successfully. The malformed checkpoint URI was also fixed and live-tested. The exact destination hostname resolves to public `20.150.246.47` from Spark under Allow and fails DNS resolution under OAP; no public connection was attempted. No documented same-workspace private custom Kafka bootstrap/advertised-broker route was demonstrated. A supported Fabric mechanism covering those actual hosts, or a new user-approved topology decision, is required; do not invent a namespace ID, self-MPE, relay or preview destination.

MCAP CLI access is verified; the corporate MCP denial is not a current authorization blocker. Workspace policy is now Deny/Deny and must not be reopened. Full GET/PUT/readback preserves both sections, uses `If-Match` whenever supplied, and records the lack of atomic concurrency protection when the GA service supplies no ETag. Approved private dependencies passed under OAP; VM private API/OneLake passed after inbound Deny; the authorized public negative returned the exact inbound-policy error. Keep compute idle after the bounded source fixture diagnostic. No automatic resume, topology change, SKU increase or deletion is authorized.

All implementation artifacts are persisted in this isolated worktree but are **untracked/uncommitted**, because the repository began empty and no commit or PR was requested. `.deployment\` evidence is intentionally ignored by Git and retained locally; no secrets or bearer tokens are included in the exported evidence. The retained B1 plan, Bastion, NAT, two public IPs, seven PEs, disks, storage, Key Vault and DNS still incur charges. The previously recorded hourly retail rates imply an incomplete fixed retained subtotal of about **$0.332/hour** (about **$242.36 per 730-hour month**), excluding disks, storage, Key Vault, DNS and usage charges; this is neither a fresh quote nor a total bill.

The latest VM source fixture is acknowledged under Deny/Deny, but it does not establish downstream ingestion. Raw OneLake publication/readback of that fixture, Spark-private KQL/Kafka consumption and the complete 900/90 baseline reconciliation and per-event 20/30-second limits still require live evidence. `Export-PreservedDeploymentEvidence.ps1` verifies fresh Paused/deallocated/security state without resuming compute; it does not claim a fresh private job-list or VM process query. `Read-SourceBootstrapDiagnostics.ps1` separately captured zero active generator/canary/installer processes before the final one-event send; the source process is waited to completion before shutdown. No production notebook or baseline generator was started. Final local logs retain 24 passing Python tests, compilation and the PowerShell policy/bootstrap/private KQL guard/artifact checks; acceptance remains false.

After the idle shutdown, the approved API security scope was completed without resuming compute: `Set-ScoringFunctionPrivateOnly.ps1` used the validated VM private canary as proof, disabled the Function public endpoint and verified a public network restriction (403 with explicit blocked-access body). The final Function state supersedes the earlier Enabled observation: it is now **Disabled**. The private endpoint remains Succeeded/Approved, and its Spark MPE was not changed. `Deploy-Azure.ps1` now reads and preserves the existing Function public-network setting and passes it through `infra/entrypoint.bicep`; a failed discovery prevents deployment rather than reopening public access. The generated `infra/entrypoint.json` was refreshed through Bicep compilation only; no infrastructure redeployment or compute resume was made.

## Request and Azure context
- Repository: `kuby49/spark_eventstream_test`; implementation only in this isolated worktree.
- Tenant: MCAP `4526cee1-4399-4e4b-9134-46fabd583527`.
- Subscription: `ME-MngEnvMCAP885977-eruskal-1` / `0a0cf761-9d5d-4862-9165-7b69c188a0ee`; region `westus3`.
- Set `AZURE_CONFIG_DIR=$HOME\.azure-mcap` in every Azure CLI shell.
- Resource group `rg_esehspark`; Fabric capacity `fabricessparktest8` F8; workspace `ES_EH_Spark_test`; VNet `vnet_esehspark`; Windows VM `vmesehspark` (<=4 vCPUs, Bastion-only); Eventstream `esspark`; Eventhouse `ehspark`; notebook `esehnotebookspark`; Lakehouse `lhspark`; Key Vault `esehsparkkv`.
- No silent renaming on name conflict, no automatic SKU increase, no resource deletion. PowerShell orchestrates Azure templates and Fabric REST. Azure resources live in the RG; Fabric SaaS items in the workspace; managed endpoints in their provider-managed infrastructure. West US 3 for regional resources; Fabric PLS/DNS at required global location.
- Preflight all tenant/Fabric admin roles/settings, home region, supported capacity region, RBAC/MFA-compliant ARM write, F8 quota/availability, VM/API SKUs, providers, name availability, endpoint rights, and live cost breakdown before creation. Account cache is not proof of ARM write authorization. Stop at a real authorization/user decision gate without weakening Conditional Access; continue only independent safe implementation/tests.

## Approved topology and data contracts
1. VM Python generator -> Eventstream custom endpoint SOURCE -> original full stream -> Eventhouse preprocessing destination -> `ehspark.RawEvents` -> OneLake availability -> `lhspark` read-only Delta shortcut.
2. The same source -> temperature `>80.0` filter -> derived stream -> custom endpoint DESTINATION -> Kafka Structured Streaming input to Spark notebook -> for each event, query Eventhouse `RawEvents` for the same equipment in inclusive event-time `[trigger - 20 minutes, trigger]`, average unique message identities, persist result, write `Events`, call scoring HTTPS API once per event, then write `Scores` or `Quarantine`.
- Keep custom endpoint destination -> Kafka Structured Streaming. Do not substitute preview native Notebook destination or a polling/relay redesign. Verify current documentation and live support: choose-network docs claim custom endpoint source/destination support while a setup table says destination unsupported. Record conflict and actual evidence; a failed exact route is a hard gate, not a bypass.
- Eventstream does not make external outbound connections. Spark is the Kafka consumer and initiates the connection. Prove a real Spark-managed-network private Kafka route with real bootstrap/topic/credentials and advertised broker hosts under OAP; VM private endpoint does not supply that route. Never invent backing Event Hubs IDs or use unsupported/unapproved route mechanisms.
- Keep OAP. Eventstream OAP documentation is preview; disclose it and keep required traffic internal to same workspace. Workspace Private Link (WSPL) and OAP are both required. Test private access before denying public workspace access.
- Generator timestamp to independently readable Delta row: every `Events` row <=20 s; every `Scores` row including API <=30 s. Baseline exactly 30/minute for 30 minutes.
- Generator uses UUID `message_id`, UUID `run_id`, equipment IDs 0..100 inclusive, temperature 0..100 inclusive, UTC RFC3339 timestamps. Exactly 10% high (`>80`) per minute: 3 high, 27 <=80 at 30/minute, paced/randomized. Permit only positive rates divisible by 10; reject others. Persist manifest of sends/attempts/acks/failures; retain original timestamp and ID across retries.
- Only derived 90 high events produce Events/Scores, not all 900. Outputs: `Events(message_id STRING,run_id STRING,equipment_id INT,message_timestamp TIMESTAMP,calculated_temperature DOUBLE)` and `Scores(message_id STRING,run_id STRING,equipment_id INT,message_timestamp TIMESTAMP,score INT [1,100])`.
- Add `ProcessingAudit` and `Quarantine`; raw payload plus run/message IDs and ingestion metadata/provenance in `RawEvents`. Quarantine malformed/failed events with key if available, payload/provenance, stage/reason/retry/time/replay/resolution.

## API requirements
- Separate Python HTTP-triggered Azure Function on dedicated Linux App Service B1, Always On. Verify exact SKU/runtime first; no expensive fallback. Fabric MPE for approved API site. VNet integration to private storage/Key Vault; private endpoints for API and storage as applicable. Disable public API/dependency access after private proof; private SCM deployment from VM when locked. Function-specific key in Key Vault; never expose keys in URL, source, state, logs, or notebook output.
- `POST https://<app>.azurewebsites.net/api/score`, TLS validated, `x-functions-key` header. JSON: message_id/run_id STRING, equipment_id INT, timestamp UTC RFC3339, calculated_temperature finite number. `Idempotency-Key` derives from run/message. Return matching IDs and integer score 1..100.
- Durable idempotency in Azure Table Storage: conditional create; repeated/concurrent request returns persisted score; same key/different payload returns 409. Managed identity storage auth; no account-key secret.
- Client total budget 8 s, <=2 attempts, bounded connect/read timeout/jitter. Retry only transient transport/408/429/retryable 5xx within remaining deadline; honor Retry-After only within budget. Explicit errors for other 4xx or invalid/mismatched/out-of-range/noninteger response.

## Infrastructure and security
- Versioned templates, invoked by PowerShell, for capacity, VNet/subnets/NSGs, private endpoints/DNS zone groups, Windows VM/NIC/disks, Bastion, Key Vault, API and storage.
- VM default `Standard_D2s_v5`, Windows Server 2022, subject to availability; no public IP/RDP, RDP only via `AzureBastionSubnet` >=/26. Bastion Basic browser access. Separate VM, PE and delegated App Service integration subnets; reserve multi-IP Fabric PE capacity. NAT Gateway for explicit VM bootstrap/auth/management outbound only, never workspace-data fallback.
- Fabric PLS: `Microsoft.Fabric/privateLinkServicesForFabric`, global, tenant/workspace configured; VNet PE subresource is workspace; private DNS `privatelink.fabric.microsoft.com` and documented workspace-specific GUID hostname. Discover actual Eventstream/Eventhouse DNS; do not assume portal zone covers it or use hosts overrides. KV `privatelink.vaultcore.azure.net`; API `privatelink.azurewebsites.net` including SCM; required storage private zones. Test Spark-managed DNS independently.
- KV: RBAC, soft delete, purge protection, private access, least-privilege managed identities for VM/API. Source/destination SAS and API key only in KV. Notebook credential helper uses approved KV MPE/required connection and supported runtime identity/token/secret path. No public Entra token endpoints from isolated Spark. Pre-stage runtime-compatible wheels/Kafka dependencies via Fabric Environment; no public PyPI/Maven under OAP.
- Preserve authenticated Azure/Fabric control-plane recovery routes. Do not claim all management endpoints are private. OAP and WSPL policy PUTs must be full replacement-safe: GET full policy, preserve inbound/outbound settings, then PUT explicit values with ETag **where supported**. When the GA service supplies no ETag, ensure no concurrent writer, disclose the caveat, and verify the full immediate readback; never invent a header or perform a partial PUT.
- Prove private DNS, TLS, authentication and data canaries for VM source; Spark Kafka, private KQL, API+Key Vault MPEs, and same-workspace Lakehouse under final policy. Then deny workspace public access. Wait for convergence and test authorized public network rejection and final private access. Never automatically reopen public access on failure.

## Spark, KQL and persistence
- Deploy notebook as `.ipynb` with `metadata.trident.lakehouse` binding to `lhspark` (source comment is insufficient); UTC; fully qualified tables and ID-based OneLake paths.
- One continuously running Kafka Structured Streaming consumer, supported group/identity, durable OneLake checkpoints, explicit schema, short microbatch; warm and ready before generator. Fit concurrent Eventstream/Eventhouse/Spark on fixed F8 with headroom; OAP has no starter pools.
- Parse JSON once with predefined `StructType`; validate IDs, UTC, finite temperature, equipment, and rate; malformed input -> Quarantine with Kafka provenance. Per-event parameterized KQL against Eventhouse, not OneLake; inclusive event-time window, unique IDs, never `now()`/future events. Record query snapshot/time and trigger raw visibility. For asynchronous branches, bounded visibility check within deadlines and explicitly report missing late visibility.
- Successful empty/null KQL result alone allows trigger-temperature fallback and audit. Query/auth/network errors are not empty results: bounded transient retry then quarantine. Persist calculation before API; write Events independently of API success; call API once per event; then Scores or Quarantine. A failure to persist audit/quarantine/output fails batch and does not advance checkpoint. Terminal quarantine must not block other records. Delta MERGE by run/message; durable stage/results for replay/partial recovery. Stored calculated value avoids changed payload on retries. Do not claim cross-table atomicity or exactly-once HTTP; retryable delivery plus durable API idempotency provides one logical score.
- Enable Eventhouse OneLake availability for RawEvents at supported minimum 5 minutes; observe actual publication/reconcile before suspend. This freshness target (documented 5 min–3 h) is not the 20/30 s source latency SLA. Use read-only Delta shortcut; no second Spark-maintained raw copy.

## Deployment stages and acceptance
1. Preflight identity/rights/quota/name/regions/tenant settings/current schemas/live cost before creating resources.
2. Deploy infrastructure/capacity/providers only with authorization; no unrelated tenant changes.
3. Configure workspace/capacity, Lakehouse, Eventhouse/KQL schema/mapping, policies, API, Environment/notebook and Eventstream REST graph: original/filter/custom source/destination.
4. Approve all workspace/API/KV/dependency private routes; enable OAP; prove DNS/TLS/auth/data canaries.
5. After VM private proof, deny workspace public access using GET-full-policy then preserve inbound+outbound and PUT explicit defaults with ETag where supported.
6. Wait convergence; test authorized public rejection and final Spark Kafka/KQL/API/KV/output private behavior. Do not reopen public access automatically.
7. Baseline run, fault fixtures, evidence, draining, OneLake raw publication and shutdown.
- Baseline acceptance: 900 unique sent/raw, 90 high triggers, 90 Events and 90 Scores, no unexplained failures/missing/duplicate output keys. Reconcile sets by run/message, not only totals: generated, send-acked, raw, consumed, Events, API attempts, scored, quarantined, replayed, unresolved. Distinguish physical deliveries from distinct identities. Outcomes are disjoint: valid score, API quarantine with Events, or pre-enrichment failure; in-flight is unresolved, never success. Join by run/message and verify equipment/time, payload, KQL/fallback, score 1..100.
- Prove every Events latency <=20 s and every Scores latency <=30 s (not percentile-only), using generator timestamp to independently readable Delta row. Report max, percentiles and counts. Spark/API/consumer prewarmed; startup excluded from per-event measurements.
- Isolated fixtures: 80 vs >80; no history; window bounds; same equipment/time with distinct IDs; duplicates; API timeout/5xx/invalid; restart; partial writes; quarantine replay. Evidence includes redacted inventory/manifests/query results/latency/policy/access/reconciliation/pass/fail before suspension.
- Private proof: VM and Spark private DNS plus protocol success. Public negative must use authorized workspace/source/destination request and prove network rejection (bad credentials alone is not proof). API/KV public denial; unapproved external Spark blocked by OAP while approved routes work.

## Shutdown and cost
- F8 is pay-as-you-go. Live West US 3 cost breakdown must include F8, Windows VM/disks, Bastion/public IP, NAT, API B1, storage, KV, private endpoints, including idle charges. Report Fabric F8 from the actual capacity meter, not an empty F8 SKU filter.
- After acceptance: stop generator, drain Spark, export evidence, wait for raw mirror/publication, stop query, suspend exactly the created capacity, deallocate exactly the created VM. Retain all resources/data. If drain fails, report unresolved state and recovery steps, not success.
- Bastion, NAT, PE, App Service plan, IP, disks, storage still incur idle costs; stopping Function does not stop B1 plan charges. Document remaining cost and resume procedure. No deletion or new recurring automation without authorization.

## Deliverables
`.azure/deployment-plan.md`; `infra/` templates/parameters; PowerShell preflight/deploy/Fabric config/VM bootstrap/network validation/acceptance/reconciliation/start/stop scripts; `src/generator`; `src/scoring_api`; Fabric Eventstream definitions/notebook/Environment libraries/KQL/table definitions; Python/PowerShell tests; README/runbook for deploy/resume/support conflicts/rotation/replay/costs/limits/evidence.

All scripts must be rerunnable with name-to-ID discovery, LRO polling/Retry-After, explicit errors and redacted state. Fabric create-item POST can return 202 LRO, not item JSON. Never delete/recreate to hide errors.

## Validation proof
Initial artifact/template validation proof and the passing targeted regression checks are recorded in Section 7. Deployment acceptance is still incomplete; the current Spark canary failure and Kafka private-route gate remain open.

## Workflow
Follow azure-prepare -> azure-validate -> azure-deploy. Do not execute deployment until validation passes. After validation, deploy only under this approved contract; run acceptance and approved idle shutdown.
