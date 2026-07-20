# Orbtr P2P Task Mesh – Operations & Execution (v2)

> A flat, ephemeral-friendly P2P task mesh running over **Orbtr Core VL1** with a **Ledger-as-Directory (LAD)**. This version adds full **task management & execution** mechanics: queues, matching, leases, retries, preemption, work stealing, draining, autoscaling, and observability.

---

## 0) Mental Model

* **Identity:** `NodeID = b32(ed25519 pubkey)`; X25519 derived for Noise.
* **Overlay (VL1):** UDP, Noise XK → AEAD (ChaCha20-Poly1305 or AES-GCM-SIV), NAT hole‑punch; **relay** is an opportunistic capability any node may advertise.
* **Directory:** **LAD** (append-only signed records) replicated over VL1; state = membership + roles + endpoints + capacity.
* **Discovery:** Query LAD for nodes that advertise specific roles and adequate capacity.
* **Scheduling:** Gateways publish **TaskOffers** (datagram pub/sub); executors **Bid**; gateway **Assigns** via reliable streams with **leases** + **fencing**.
* **Elastic Roles:** Dedicated or opportunistic; nodes may activate roles dynamically when demand exceeds supply.

---

## 1) Flat Mesh Topography (Example)

> Pure peer-to-peer: no hierarchy, no static addresses. Roles are capabilities announced in LAD; nodes may appear/disappear at any time.

```mermaid
graph LR
  subgraph Clients / Internet
    C1[Frontend A]
    C2[Core App B]
  end

  subgraph Orbtr Flat Mesh (VL1)
    N1((Node N1)):::gw
    N2((Node N2)):::id
    N3((Node N3)):::auth
    N4((Node N4)):::worker
    N5((Node N5)):::relay
    N6((Node N6)):::general
  end

  classDef gw fill:#1d3557,color:#fff,stroke:#0a1a2f
  classDef id fill:#2a9d8f,color:#fff,stroke:#0a1a2f
  classDef auth fill:#457b9d,color:#fff,stroke:#0a1a2f
  classDef worker fill:#8d99ae,color:#fff,stroke:#0a1a2f
  classDef general fill:#adb5bd,color:#000,stroke:#0a1a2f

  C1 -->|Intent: verify_identity| N1
  C2 -->|Intent: authenticate| N1

  %% Mesh links (illustrative)
  N1 --- N2
  N1 --- N3
  N1 --- N5
  N2 --- N4
  N3 --- N5
  N4 --- N6
  N5 --- N6
```

* Nodes self‑announce roles/capacity via LAD and may change at runtime.
* **Relay** selection is dynamic (latency/loss/price) with hysteresis to avoid flapping.

---

## 2) Orbtr VL1 Overlay (On‑Wire & Sessions)

### VL1 Packet (v1)

```
+-------+-------+---------+---------------------+
|Ver=1  |Flags  |Type     |SessionID (64b)      |
+-------+-------+---------+---------------------+
|Nonce (96b)
+--------------------------------------------------+
|Ciphertext (AEAD; AAD = header fields)
```

* **Types:** `HELLO`, `HANDSHAKE1`, `HANDSHAKE2`, `DATA`, `KEEPALIVE`, `CLOSE`, `REKEY`.
* **Cipher:** ChaCha20‑Poly1305 or AES‑GCM‑SIV. Rekey after 10m or 1 GiB.
* **NAT & Multipath:** STUN‑lite via peers, simultaneous punches, path scoring `RTT_ewma + loss_penalty`, switch if ≥30% better. Relay tickets when needed.
* **Data modes:** **Streams** (reliable) for control/tasks/results; **Datagrams** for gossip/offers.

---

## 3) Ledger‑as‑Directory (LAD)

**MemberRecord (CBOR, subject‑signed)**

```cbor
MemberRecord = {
  "v":1, "nid":tstr, "pk_ed":bstr,
  "roles":[* tstr],                ; e.g., ["identity","authentication","gateway","worker","relay"]
  "handlers":{ tstr=>tstr }?,      ; role => semver
  "cap":{ "cpu":float, "ram_mb":uint, "disk":float, "net":float, "maxp":uint }?,
  "endpoints":[ {"proto":"udp","host":tstr,"port":uint,"prio":uint}*,
                  {"proto":"relay","ticket":bstr,"relay_nid":tstr}? ],
  "rendezvous":{ "reflex":[* tstr], "local":[* tstr] }?,
  "claims":{ tstr=>tstr }?,
  "exp":uint, "nonce":bstr, "sig":bstr
}
```

**RevokeRecord / RotateRecord** manage revocation and key rotation. At state build, keep latest non‑expired MemberRecord not revoked; apply rotations.

**Replication:** gossip tip (datagrams), range sync (streams), periodic checkpoints. Record size ≤ 2–4 KiB; leaky bucket per `nid`; optional PoW/fee in permissionless mode.

**Join (no static addresses):** Invite token carries a **NodeID** and optional **relay ticket**. New node dials by NodeID (punch/relay), syncs checkpoint+tail, posts its MemberRecord.

---

## 4) Control Plane (Discovery, Gossip, Policy)

* **Directory queries:** Find executors by role and capacity threshold; prefer fresh `exp`.
* **Gossip cadence:** \~2s for capacity/version beacons.
* **Policy (OPA/CEL) example:**

```
request.type == "verify_identity" &&
request.tenant in ["t1","t2"] &&
request.slo.prio <= 7
```

* **Revocation:** CRL via ledger + gossip; peers drop revoked nodes immediately.

---

## 5) Task Model, Queues & Scheduling

### 5.1 Task object

`Task = { tid, type, tags[], tenant, slo{lat_ms, prio, rel}, idem, payload_ref }`

### 5.2 Queues

* **Per‑role priority queues** at the gateway.
* Score per task for ordering:

```
score = w_prio*prio + w_deadline*EDF(deadline) - w_age*wait_ms
```

* **EDF(deadline)** = inverse time to deadline; late tasks get a bump.
* **Tenant fairness:** token buckets per tenant (rate, burst) and per‑tenant max in‑flight.

### 5.3 Offers & Bids

* Gateway publishes `TaskOffer` on `tasks.<role>.vX` (datagrams).
* Executors subscribe by role; they bid only if **admission predicate** passes:

```
admit = capacity.score >= S && predictedETA <= SLO.latency
```

### 5.4 Matching & Assignment

* Rank bids by: capacity score, ETA vs SLO, handler version match, diversity (penalize recent winners), and freshness of MemberRecord.
* Select **primary** (+ optional **standby**). Send `Assign{lease_ms,fence}` over a dedicated stream; executor replies `Ack`.

### 5.5 Execution & Heartbeats

* Executor runs handler; emits `Progress{pct,msg}` periodically.
* **Leases** extend on heartbeat; if lease expires or progress stalls → gateway may **preempt** (for low‑prio) or **reassign**.
* **Idempotency & fencing:** `idem` + `fence` ensure retries don’t double‑apply side effects.

### 5.6 Completion

* Executor returns `Result{status,out,err,fence}`; gateway validates fence and returns to client.
* Optional: commit a **result header hash** to LAD for audit (without payloads).

---

## 6) Resilience: Failure, Stealing, Draining, Churn

* **Retries:** On error/timeout, re‑offer with a new fence. Old results with old fence are ignored.
* **Work stealing:** Idle nodes subscribe broadly; when queues swell and `availability(role)` is low, they **Activate(role)** and bid.
* **Drain mode:** Before shutdown, node posts quick LAD update `accept:false`, `qdepth=0`; finishes in‑flight leases, then exits.
* **Backoff on flapping:** Exponential backoff per `nid` and a decaying penalty in bid ranking.
* **Preemption:** Low‑priority tasks may be paused/reassigned if high‑priority work arrives and SLO breach is predicted.

---

## 7) Autoscaling Signals

* Gateways export: queue depth by role/tenant/SLO bucket, P50/P95 wait, timeout counts, drop counts, bid acceptance ratios.
* Simple scaler rule:

```
if P95_wait(role) > target for K windows => encourage Activate(role)
if utilization(role) < low_water for K windows => Deactivate(role)
```

* Handlers may be fetched on demand (OCI artifact + sigstore verify) to enable truly stateless generalists.

---

## 8) Security Model

* **AuthN:** Node identities via ed25519; Noise XK binds peers.
* **AuthZ:** OPA/CEL at gateway and executor; scopes/tenants checked twice.
* **DoS controls:** per‑peer rate limits, tiny PoW/macaroons on offers, banlist via revocation.
* **Replay:** Session nonce windows; LAD records use short `exp` and monotonic `nonce`.
* **Privacy:** Payload minimization; use content‑addressed blobs with per‑task envelope keys.

---

## 9) Observability

* **Per‑node:** Prometheus metrics (capacity, queue depth, latency, success, activations, lease expiries).
* **Mesh‑wide:** Heatmaps of role availability & SLO adherence; bid/assign/complete traces.
* **Tracing:** W3C trace context across gateway⇄executor; propagate span IDs in `TaskOffer` and `Result`.

---

## 10) Config (Example)

```yaml
orbtr:
  noise:
    prologue: "orbtr-vl1-v1"
    rekey_after: "10m"       # or 1 GiB
  nat:
    punch_hello_ms: 250
    keepalive_s: 5
    suspect_s: 10
    dead_s: 20

mesh:
  roles:
    preferred: ["identity","authentication"]
    opportunistic: ["worker","relay"]
  gateway:
    enabled: true
  gossip_interval_ms: 2000
  capacity:
    weights: { cpu: 0.35, ram: 0.25, disk: 0.1, net: 0.1, qdepth: 0.1, lat: 0.1 }
    max_parallel: 16
  relay_selection:
    score: "rtt_ewma + loss_penalty + price_weight"
    improve_switch_threshold: 0.30
    stickiness_s: 20
  ledger:
    mode: "crdt"  # crdt|poa|pos
    checkpoint_every: 300
    path: "/var/lib/mesh/ledger.log"
```

---

## 11) Message Schemas (CBOR)

**TaskOffer**

```cbor
TaskOffer = {
  "tid":tstr,
  "type":tstr,
  "tags":[* tstr],
  "slo":{ "lat_ms":uint, "prio":uint, "rel":float },
  "idem":tstr,
  "hint":bstr
}
```

**Bid / Assign / Ack / Progress / Result**

```cbor
Bid     = { "tid":tstr, "nid":tstr, "score":uint, "eta_ms":uint, "role_ver":tstr }
Assign  = { "tid":tstr, "lease_ms":uint, "fence":tstr, "keys":bstr? }
Ack     = { "tid":tstr, "accepted":bool }
Progress= { "tid":tstr, "pct":uint, "msg":tstr? }
Result  = { "tid":tstr, "status":"OK"|"ERR", "out":bstr?, "err":tstr?, "fence":tstr }
```

---

## 12) Sequence Diagrams

### 12.1 Task Lifecycle (flat mesh)

```mermaid
sequenceDiagram
  participant FE as Frontend
  participant GW as Gateway
  participant EX as Executor (role node)

  FE->>GW: POST /intent {type, tags, SLO, idem}
  GW->>GW: Enqueue task; compute EDF+prio score
  GW->>EX: Publish TaskOffer (topic tasks.<role>.vX)
  EX-->>GW: Bid {score, eta}
  GW->>EX: Assign {lease_ms, fence}
  EX-->>GW: Ack
  EX->>EX: Run handler; emit Progress
  EX-->>GW: Result {status,out,fence}
  GW-->>FE: 200 OK {result}
```

### 12.2 Membership Update (optional validators)

```mermaid
sequenceDiagram
  participant N as Node
  participant V as Validator (Proposer)
  participant V2 as Validator (Committee)
  participant V3 as Validator (Committee)

  N->>V: Submit MemberRecord (CBOR, signed)
  V->>V2: Propose Block{records:[MemberRecord]}
  V->>V3: Propose Block
  V2-->>V: Attest
  V3-->>V: Attest
  V-->>N: Block Finalized {height,hash}
  V->>All: Gossip tip {height,hash}; periodic checkpoints
```

---

## 13) Runbook (Ops)

* **Scale out executors:** add nodes; ensure they announce roles; watch queue P95.
* **Graceful drain:** set `accept:false`, wait for in‑flight leases < threshold, stop process.
* **Hot role shortage:** raise `availability(role)` target or lower activation threshold so more nodes activate the role.
* **Relay saturation:** prefer nodes with `relay:true` and high score; rotate if a candidate is ≥30% better.
* **Incident response:** quarantine misbehaving `nid` (revocation record), invalidate leases, re-offer tasks, inspect traces.
