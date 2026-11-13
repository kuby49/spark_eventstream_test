# Fabric Eventstream and Spark Streaming Setup

Create all below in a resource group rg_esehspark

## Capacity and Workspace

- Create a new Fabric capacity: `fabricessparktest8`.
- Create a new workspace: `ES_EH_Spark_test`, on that capacity.
- The workspace should be behind **Private Link** and **OAP-protected**.
- OAP = Outbound Access Protection of the Fabric Workspace
- Block **Public access** once tested **Private access** works =

## Network and Virtual Machine

Set up a VNet (`vnet_esehspark`) with a Windows VM (`vmesehspark`) where the **Workspace Private Linke ** of the fabric workspace is exposed and the Fabric-protected workspace can be accessed from this VM over private endpoints

VM access to be made through **Bastion**

**Validate** that fabric workloads support private network configuration

**Private DNS resolution** as per the Fabric WSPL documentation.

## Workspace Items

In the workspace, create:

1. An **Eventstream** called esspark with a **custom endpoint** where the messages will be sent from the VM over the private route.
2. An **Eventhouse** called ehspark receiving the main original stream of event stream
3. The **Spark Streaming job / notebook** called esehnotebookspark
4. A lakehouse called lhspark

## Explicit End to end dataflow

There are *three* paths

```text
1. Generator → Eventstream (esspark) → Eventhouse (ehspark) → Lakehouse mirror of eventhouse (oneLake availability of ehspark data)
2. Eventstream (esspark) derived stream -> Spark streaming job (esehnotebookspark) -> RUN KQL Query (ehspark) -> API call -> Lakehouse (lhspark table Scores)
3. -> RUN KQL Query (ehspark) -> Lakehouse (lhspark table Events)
```

**Derived stream** contains events from the received stream where temperature is bigger than 80.0 degrees

**KQL query** for the given event should return for the given equipment_id for which the event was detected the average of temperatures for the last 20 minutes from the trigger timestamp

if **KQL query** returns no data, use the temperature of the event received

## Additional Components and Streaming Logic

### 1. Sample Data Generator

Write a **Python** program that generates sample data for the Eventstream custom endpoint:

- Equipment IDs between **0 and 100**.
- Temperatures between **0 and 100**, with **10%** of temperatures of messages emitted in a minute being greater than **80 degrees**.
- A configurable number of messages sent per minute, starting with **30 messages per minute**.

Run it on the VM `vmesehspark`.

This is an example payload

```json
{
  "message_id": "unique-id",
  "equipment_id": 42,
  "timestamp": "2026-11-02T20:00:00Z",
  "temperature": 85.4
}
```

### 2. REST API

Set up a simple application that exposes a **REST API**:

- Receives `equipment_id`, the message timestamp, and the **KQL query** calculated (average of last 20 minutes) temperature value in the payload

- Returns a random number between **1 and 100**

Endpoint, request/response JSON, authentication, timeout, and retry policy i let you choose

### 3. Managed Private Endpoint

Set up a **managed private endpoint** from the workspace to this API so that Spark can invoke it.

### 4. Spark Streaming Output

Have the Spark streaming code write two tables in the lakehouse

In case of API call failure spark should quarantine the record

API invocation should be on a per event basis

Latency expectations, no more than 20 seconds for Data Flow 2, and no more than 30 seconds (including API answr time) for Data Flow 3

| Table | Columns |
| --- | --- |
| Events table | `equipment_id`, message timestamp, calculated temperature value |
| Scores table | `equipment_id`, message timestamp, number returned by the API |

Select yourself the appropriate field types for above tables

## Deployment, cost, and operations

Azure region: Westus 3,

capacity SKU: F8,

VM size: no more than 4 cores, Windows OS

deployment method : Powershell script,

secret storage: Azure Keyvalut (`esehsparkkv`)

Create all above in a resource group rg_esehspark

## Acceptance Criteria

Run at 30 messages/minute for 30 minutes. Reconcile sent, ingested, processed, failed, and duplicate message counts. Verify both output tables correlate by equipment id and message timstamp, API values are within 1–100, and processing meets the agreed latency target. Confirm approved private access works and unapproved public access is blocked.
