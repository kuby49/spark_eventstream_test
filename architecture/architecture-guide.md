# Deployed Fabric streaming architecture

Prepared 5 November 2026. Azure resource inventory was refreshed read-only in the MCAP tenant. Fabric item relationships and network behavior are grounded in deployed source and retained session evidence; this is not a new end-to-end network certification.

## Diagrams

- **streaming-data-architecture.svg**: the three data paths and processing dependencies.
- **streaming-network-architecture.svg**: private access, provisioned Azure services and network boundaries.
- Matching **.excalidraw** files are editable in https://aka.ms/excalidraw.

### Explicit perimeters

- **Blue VM perimeter**: the Windows VM and its guest emitter/private-client execution. Bastion, NAT and private endpoints are surrounding network services, not processes running in the VM.
- **Purple Fabric perimeter**: the capacity-assigned workspace, Eventstream, Eventhouse, Lakehouse, notebook and Fabric-managed Spark network. This boundary is separate from the customer VNet.
- **Green API perimeter**: the scoring Function and its hosting plan. API supporting storage is included in the logical data diagram and separately outlined in the network diagram because it has its own network/data-plane boundary.
- **Shared dependencies**: Key Vault, customer-VNet DNS and private endpoint plumbing are shown outside the three execution perimeters. Cross-boundary arrows show the actual connections.

These are logical ownership/execution boundaries, not claims that all services share one physical network or one identical security policy.

## Three data paths

| Path | Route | Meaning |
|---|---|---|
| 1: Original/raw | VM Python emitter -> `esspark` custom input -> original stream -> `ehspark.RawEvents` -> Eventhouse OneLake availability -> `lhspark.RawEvents` shortcut | All original messages. The shortcut references Eventhouse-managed data; it is not another Spark ingestion job or a Spark-owned copy. |
| 2: Scored events | `esspark` filter `temperature > 80` -> derived custom Kafka endpoint -> `esehnotebookspark` -> KQL enrichment -> private scoring API -> `lhspark.Scores` | One API call per event. The API returns a random integer 1-100 and maintains durable idempotency. |
| 3: Enriched events | Same derived Kafka reader -> same KQL enrichment -> `lhspark.Events` | Events are persisted before invoking the API, so an API failure does not erase the enriched event. |

For each trigger, KQL computes average temperature for the same equipment over the inclusive interval from trigger timestamp minus 20 minutes to trigger timestamp. A successful empty result falls back to the trigger's temperature. Query failures are not empty results: they are quarantined. The current client KQL budget is 4 seconds and includes credential/setup work, not only server execution.

Kafka JSON-array envelopes are expanded before event filtering and processing. Run/message identities, durable processing stages and API idempotency support recovery; this does not imply a cross-table atomic transaction or exactly-once HTTP execution. `ProcessingAudit` records durable stages; `Quarantine` records failures.

## Provisioned service inventory and links

| Layer | Provisioned services | Links and purpose |
|---|---|---|
| Fabric capacity | `fabricessparktest8`, F8, West US 3 | Assigned to workspace `ES_EH_Spark_test`. |
| Fabric workspace | `esspark`, `ehspark`, `esehnotebookspark`, `lhspark` | Eventstream, Eventhouse/KQL database, streaming notebook and Lakehouse. Diagnostic notebooks also exist for probes and maintenance, not as additional production data paths. |
| Spark compute | Medium Starter Pool, autoscale 1-6 nodes; notebook dynamic executors min/initial 1, max 5 | Driver/executor: 8 cores and 56 GB each. Configured aggregate ceiling is 48 cores; it is not guaranteed allocation. Recorded stress peak was two executors plus driver, 24 cores. |
| VM network | `vnet_esehspark`, `nsg-esehspark-vm`, `nic-vmesehspark` | Customer VNet `10.74.0.0/16`. VM subnet `10.74.1.0/24`, private endpoints `10.74.2.0/24`, App Service integration `10.74.3.0/26`, Bastion `10.74.0.0/26`. |
| VM and disk | `vmesehspark`, Windows Standard_D2s_v5; `osdisk-vmesehspark`, 127 GiB Standard SSD | Runs the Python IoT simulator and private management/evidence clients. AADLoginForWindows and MDE.Windows extensions were observed. |
| Bastion | `bas-esehspark`, `pip-esehspark-bastion` | Operator browser -> Bastion -> VM RDP. The NSG permits Bastion RDP and denies other VNet RDP sources. |
| VM egress | `nat-esehspark`, `pip-esehspark-nat` | NAT attached to VM subnet. Not used to represent the proven private Eventstream data route. |
| Workspace Private Link | `pl-fabric-esehspark`, `pe-esehspark-fabric-workspace` | Exposes workspace services through the customer VNet. A workspace private endpoint is not the same resource as a Spark managed private endpoint. |
| Scoring service | `esehspark-score-api`, B1 plan `asp-esehspark-score` | Per-event authenticated HTTPS request with equipment ID, trigger timestamp and calculated average. Function-key authentication, Key Vault secrets, bounded timeout/retries, random 1-100 response. |
| Scoring access | `pe-esehspark-api`; Spark `mpe-esehspark-score-api` | VM and Spark reach the same API through distinct private access mechanisms. Function outbound VNet integration accesses supporting services. |
| Secrets | `esehsparkkv`, `pe-esehspark-kv`; Spark `mpe-esehsparkkv` | Stores endpoint/API credentials. VM/Function identities and the authorized notebook submitter use scoped access. No secrets are embedded in these diagrams. |
| Function storage | `esehsparkstore8`; `pe-esehspark-storage-blob`, `-file`, `-queue`, `-table` | Function runtime storage and durable score/idempotency data. This Azure storage account is not the Lakehouse's OneLake storage. |
| Private DNS | Fabric, App Service, Key Vault, Blob, File, Queue and Table zones | Zones are linked to the customer VNet. Managed Spark networking is a separate Fabric-managed boundary. |
| Storage event integration | Storage-associated Event Grid system topic | Present in live ARM inventory; not part of the three requested streaming paths. |

The Kafka/Event Hubs-compatible custom endpoints are Fabric/Eventstream-managed. No separately owned Azure Event Hubs namespace was provisioned for this design.

Azure resources are in `rg_esehspark`. Fabric workload items and managed endpoints are SaaS workspace-scoped artifacts; they do not appear as individual ARM resources under that resource group.

## Current security configuration

- Workspace **inbound Deny**, with approved private access proven in prior tests.
- Workspace **outbound Allow: OAP is OFF**, explicitly changed by the user after the original OAP-enabled design encountered connectivity problems. Do not label the current architecture OAP-protected.
- Scoring Function **public network access Disabled**. Prior public workspace/API tests were network-denied.
- Spark uses managed private endpoints for the scoring API and Key Vault. Kafka custom-endpoint private resolution was observed under the current configuration; do not imply that an independently owned Event Hubs MPE exists.
- Bastion and NAT deliberately have public IPs. Private workspace/API access does not imply every management surface or every Azure resource has all public connectivity disabled.

## Stress-test operations and measurement

The user-approved stress emitter schedules sends with bounded background acknowledgement tracking and bounded buffered local logging. It does not wait synchronously for each acknowledgement. Actual first-enqueue rate is reported separately from the configured rate. Recent buffered evidence can be lost on a simulator crash; incomplete crash-interrupted evidence cannot establish a passing run. This mode does not implement a new durable-outbox crash replay guarantee.

Each completed stress run drains and stops its owned stream, then runs one OPTIMIZE pass over `Events`, `Scores`, `ProcessingAudit` and `Quarantine`. The mirrored `RawEvents` shortcut is excluded. No automatic VM deallocation or capacity suspension is allowed under the current instruction.

Reported processing latency currently uses Spark timestamps **before each output's own write**, not the original independently readable-output SLA. Earlier readable-output failures remain failures.

As of the latest retained results, actual **893.92 messages/minute** was fully reconciled. A later actual **918.87/minute** run produced all 1,395 high-temperature outputs but its post-run raw-count queries received persistent HTTP 429 responses; raw ingestion count remains unavailable, not zero. That does not establish F8 exhaustion or a proven processing ceiling. Architecture generation itself did not launch or alter tests.
