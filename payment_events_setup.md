# Payment Events System — Setup & Design Decisions

**Project:** Digital Payments Event Streaming Infrastructure
**Cluster:** Local Rancher Desktop Kubernetes (kafka-0, kafka-1, kafka-2)

---

## 1. Design Decisions

**Reminder**
**SLA (Service Level Agreement):** maximum amount of time allowed for an operation to complete.
- Payment processing: 150ms
- Fraud scoring: 50ms
- Customer notification: 2s

I used the CapitecPay config as a reference and starting point for most settings. Almost all values were kept the same. Three consumer values were adjusted to meet this project's stricter SLA requirements: `session.timeout.ms`, `heartbeat.interval.ms`, and `retries` — explained in each section below.

---

### 1.1 Topic Design

I created three separate topics so each part of the system only reads what it needs:

- `digital-payment.payment-events` — all payment lifecycle events, kept for 7 years (regulatory requirement)
- `digital-payment.fraud-events` — fraud scores per payment, kept for 7 years
- `digital-payment.notification-events` — customer notifications, kept for 30 days (operational only, no compliance requirement)

I followed the `<domain>.<entity>-events` naming convention, referencing the capitec-pay config to keep it consistent and avoid name clashes on a shared cluster.

**Partitions: 10** — more partitions allow more messages to go through at the same time, helping with lag.
With each producer doing 100 Mb/s and a target of 1000 Mb/s, 10 partitions allows 10 producers in parallel. It also means up to 10 consumers can work in parallel — one per partition at a time.
I set the partition key to `accountId` so all events for the same customer land on the same partition in order, which fraud detection needs.

**Replication factor: 3** — the cluster has 3 brokers, so replication 3 means every broker holds a copy. If one goes down, data is not lost.
For payments, losing data events would cause compliance and reconciliation failures, so this was set to the minimum.

**Retention** — payment and fraud topics use `retention.ms=220752000000` (7 years), to meet the regulatory audit requirement.
Notifications use `retention.ms=2592000000` (30 days).
I didn't add size-based retention for payment or fraud because a size cap could delete legally required data before 7 years if volumes grow — compliance comes first.

**Cleanup policy: `delete`** — payment events are permanent facts. Compaction (which keeps only the latest message per key) is for state stores.
An event stream should never overwrite history, just expire it after the retention period.

**Compression: `snappy`** — payment events are JSON text which compresses well. Snappy is fast with a good ratio, allowing better latency than gzip which matters for the 150ms payment SLA.

**Min in-sync replicas: 2** — with `acks=all`, at least 2 of 3 brokers must confirm a write. This means one broker can be down and payments still go through. Requiring all 3 would block payments on any single broker failure.

**Message schema** — every event has `eventType`, `accountId` (also the partition key), `paymentId`, and `timestamp` as required fields. Payment events add `amount`, `currency`, and `channel`. Outcome events add `authorisedBy` or `validatedBy`. Failure events add `reason`. Fraud events add a `score` (0–100).

---

### 1.2 Producer Design

Three producers were designed:
- **Payment producer** → publishes to `digital-payment.payment-events`: `payment.initiated`, `payment.authorised/not_authorised`, `payment.validated/invalidated`, `payment.completed/not_completed`
- **Fraud producer** → publishes to `digital-payment.fraud-events`: `fraud.score.low/medium/high`
- **Notification producer** → publishes to `digital-payment.notification-events`: `notification.payment.approved/failed`

Key settings across all producers:

**`acks=all`** — all in-sync brokers must confirm before the producer moves on. We can't afford to lose a payment event.

**`enable.idempotence=true`** — prevents duplicate events when the producer retries after a network issue. Without this, a retry creates a duplicate payment record. Note: this only works within a single session — if the producer is restarted, Kafka treats it as a new session (Issue 7 shows what happens).

**`retries=10`** — enough to survive a Kafka leader election (30–60 seconds). CapitecPay uses 5, which I increased to 10 to meet the project's requirement for an explicit, auditable retry count. `delivery.timeout.ms=120000` caps the total retry window at 2 minutes.

**`linger.ms`** — how long to wait before sending a batch. Fraud uses `linger.ms=0` (no wait, meets the 50ms SLA). Payment uses `linger.ms=5`. Notification uses `linger.ms=10` (2s SLA gives more room).

**Serialization: JSON** — readable and easy to verify in the terminal during the POC.

---

### 1.3 Consumer Design

Three consumer groups, each independent so they can be scaled and configured separately:

**`payment-audit-consumer-group`** — reads all event types from `payment-events`, SLA is near real-time. Uses `auto.offset.reset=earliest` (must read from the beginning — missing events is a compliance failure) and `enable.auto.commit=false` (bookmark only saves after the event is fully written to the audit store, so a crash replays rather than skips). `max.poll.records=100`.

**`fraud-detection-consumer-group`** — reads from `payment-events`, SLA < 50ms. In production would only act on `payment.initiated` events — that's when fraud scoring needs to happen. CLI tool can't filter so all events show in output. Uses `auto.offset.reset=earliest` for the POC (events were already in the topic before consumers started — `latest` would have missed them). Production would use `latest`. `max.poll.records=10` for low latency.

**`notification-consumer-group`** — reads from `payment-events`, SLA < 2s. In production would only act on `payment.validated` and `payment.not_completed`. Same POC reasoning for `earliest`. `max.poll.records=50`.

All three share: `session.timeout.ms=45000` / `heartbeat.interval.ms=12000`. CapitecPay uses `session.timeout.ms=90000` and `heartbeat.interval.ms=30000`, which are valid for their system. For this project I adjusted both to meet the 50ms fraud SLA — 90s idle after a consumer crash is too long, and the heartbeat needed to be recalculated to stay strictly less than `session.timeout.ms / 3` (45000 / 3 = 15000, so 12000 was chosen).

---

## 2. Topic Creation

Commands match `payment.md`, `fraud.md`, and `notification.md`.

### 2.1 Create Topics

**Payment events:**
```bash
kubectl exec -it kafka-0 -- kafka-topics \
  --bootstrap-server kafka-service:9092 \
  --create \
  --topic digital-payment.payment-events \
  --partitions 10 \               # 10 consumers max in parallel
  --replication-factor 3 \        # one copy per broker, zero data loss on failure
  --config retention.ms=220752000000 \   # 7 years
  --config cleanup.policy=delete \
  --config min.insync.replicas=2 \
  --config compression.type=snappy \
  --config max.message.bytes=1048576
```

**Fraud events:**
```bash
kubectl exec -it kafka-0 -- kafka-topics \
  --bootstrap-server kafka-service:9092 \
  --create \
  --topic digital-payment.fraud-events \
  --partitions 10 \
  --replication-factor 3 \
  --config retention.ms=220752000000 \   # 7 years
  --config cleanup.policy=delete \
  --config min.insync.replicas=2 \
  --config compression.type=snappy \
  --config max.message.bytes=1048576
```

**Notification events:**
```bash
kubectl exec -it kafka-0 -- kafka-topics \
  --bootstrap-server kafka-service:9092 \
  --create \
  --topic digital-payment.notification-events \
  --partitions 10 \
  --replication-factor 3 \
  --config retention.ms=2592000000 \     # 30 days
  --config cleanup.policy=delete \
  --config min.insync.replicas=2 \
  --config compression.type=snappy \
  --config max.message.bytes=1048576
```

---

### 2.2 Verification (`--describe` output)

`Isr: 0,1,2` on every partition confirms all 3 brokers have a current copy — full replication working.

```
Topic: digital-payment.payment-events   TopicId: GG-AniHeRzqcPGuOIi5pYQ   PartitionCount: 10   ReplicationFactor: 3
Configs: compression.type=snappy,min.insync.replicas=2,cleanup.policy=delete,retention.ms=220752000000,max.message.bytes=1048576
  Partition: 0   Leader: 1   Replicas: 1,2,0   Isr: 0,1,2
  Partition: 1   Leader: 2   Replicas: 2,0,1   Isr: 0,1,2
  Partition: 2   Leader: 0   Replicas: 0,1,2   Isr: 0,1,2
  Partition: 3   Leader: 0   Replicas: 0,2,1   Isr: 0,1,2
  Partition: 4   Leader: 2   Replicas: 2,1,0   Isr: 0,1,2
  Partition: 5   Leader: 1   Replicas: 1,0,2   Isr: 0,1,2
  Partition: 6   Leader: 2   Replicas: 2,0,1   Isr: 0,1,2
  Partition: 7   Leader: 0   Replicas: 0,1,2   Isr: 0,1,2
  Partition: 8   Leader: 1   Replicas: 1,2,0   Isr: 0,1,2
  Partition: 9   Leader: 2   Replicas: 2,1,0   Isr: 0,1,2
```

```
Topic: digital-payment.fraud-events   TopicId: I6T7i1LKScevJYJqqE_Cjw   PartitionCount: 10   ReplicationFactor: 3
Configs: compression.type=snappy,min.insync.replicas=2,cleanup.policy=delete,retention.ms=220752000000,max.message.bytes=1048576
  Partition: 0   Leader: 1   Replicas: 1,2,0   Isr: 1,2,0
  Partition: 1   Leader: 2   Replicas: 2,0,1   Isr: 2,0,1
  Partition: 2   Leader: 0   Replicas: 0,1,2   Isr: 0,1,2
  Partition: 3   Leader: 0   Replicas: 0,2,1   Isr: 0,2,1
  Partition: 4   Leader: 2   Replicas: 2,1,0   Isr: 2,1,0
  Partition: 5   Leader: 1   Replicas: 1,0,2   Isr: 1,0,2
  Partition: 6   Leader: 0   Replicas: 0,2,1   Isr: 0,2,1
  Partition: 7   Leader: 2   Replicas: 2,1,0   Isr: 2,1,0
  Partition: 8   Leader: 1   Replicas: 1,0,2   Isr: 1,0,2
  Partition: 9   Leader: 0   Replicas: 0,2,1   Isr: 0,2,1
```

```
Topic: digital-payment.notification-events   TopicId: uk1BE3PBRqywIzjCXQh0XQ   PartitionCount: 10   ReplicationFactor: 3
Configs: compression.type=snappy,min.insync.replicas=2,cleanup.policy=delete,retention.ms=2592000000,max.message.bytes=1048576
  Partition: 0   Leader: 2   Replicas: 2,0,1   Isr: 0,1,2
  Partition: 1   Leader: 0   Replicas: 0,1,2   Isr: 0,1,2
  Partition: 2   Leader: 1   Replicas: 1,2,0   Isr: 0,1,2
  Partition: 3   Leader: 1   Replicas: 1,2,0   Isr: 0,1,2
  Partition: 4   Leader: 2   Replicas: 2,0,1   Isr: 0,1,2
  Partition: 5   Leader: 0   Replicas: 0,1,2   Isr: 0,1,2
  Partition: 6   Leader: 0   Replicas: 0,2,1   Isr: 0,1,2
  Partition: 7   Leader: 2   Replicas: 2,1,0   Isr: 0,1,2
  Partition: 8   Leader: 1   Replicas: 1,0,2   Isr: 0,1,2
  Partition: 9   Leader: 2   Replicas: 2,0,1   Isr: 0,1,2
```

Topic list confirmed:
```
digital-payment.fraud-events
digital-payment.notification-events
digital-payment.payment-events
```

---

## 3. Producer Setup

Commands match `payment.md`, `fraud.md`, and `notification.md`.

### 3.1 Payment Producer

```bash
kubectl exec -it kafka-0 -- kafka-console-producer \
  --bootstrap-server kafka-service:9092 \
  --topic digital-payment.payment-events \
  --property parse.key=true \
  --property key.separator=: \
  --producer-property acks=all \                              # all brokers must confirm
  --producer-property retries=10 \                           # survives a leader election
  --producer-property max.in.flight.requests.per.connection=5 \
  --producer-property enable.idempotence=true \              # no duplicate events on retry
  --producer-property compression.type=snappy \
  --producer-property linger.ms=5 \                         # small batch wait, below 150ms SLA
  --producer-property batch.size=16384 \
  --producer-property delivery.timeout.ms=120000 \          # 2 min total retry window
  --producer-property request.timeout.ms=30000
```

I produced two payment journeys keyed on `accountId`. The producer was run twice by mistake (see Issue 7) so 7 events ended up in the topic instead of 6.

**Journey 1 — Successful payment (ACC-001):**
```
ACC-001:{"eventType":"payment.initiated","accountId":"ACC-001","paymentId":"PAY-001","amount":1500.00,"currency":"ZAR","channel":"digital","timestamp":"2026-09-28T08:00:00Z"}
ACC-001:{"eventType":"payment.authorised","accountId":"ACC-001","paymentId":"PAY-001","authorisedBy":"auth-engine","timestamp":"2026-09-28T08:00:00.080Z"}
ACC-001:{"eventType":"payment.validated","accountId":"ACC-001","paymentId":"PAY-001","validatedBy":"compliance-engine","timestamp":"2026-09-28T08:00:00.120Z"}
ACC-001:{"eventType":"payment.completed","accountId":"ACC-001","paymentId":"PAY-001","timestamp":"2026-09-28T08:00:00.145Z"}
```

**Journey 2 — Failed payment (ACC-002):**
```
ACC-002:{"eventType":"payment.initiated","accountId":"ACC-002","paymentId":"PAY-002","amount":50000.00,"currency":"ZAR","channel":"digital","timestamp":"2026-09-28T08:01:00Z"}
ACC-002:{"eventType":"payment.not_authorised","accountId":"ACC-002","paymentId":"PAY-002","reason":"insufficient_funds","timestamp":"2026-09-28T08:01:00.043Z"}
```

Partition 0 received 5 messages (all ACC-001 events including the duplicate). Partition 4 received 2 messages (all ACC-002 events). All other partitions: 0. Ordering per account confirmed.

---

### 3.2 Fraud Producer

```bash
kubectl exec -it kafka-0 -- kafka-console-producer \
  --bootstrap-server kafka-service:9092 \
  --topic digital-payment.fraud-events \
  --property parse.key=true \
  --property key.separator=: \
  --producer-property acks=all \
  --producer-property retries=10 \
  --producer-property max.in.flight.requests.per.connection=5 \
  --producer-property enable.idempotence=true \
  --producer-property compression.type=snappy \
  --producer-property linger.ms=0 \                         # no batch wait — meets 50ms fraud SLA
  --producer-property batch.size=16384 \
  --producer-property delivery.timeout.ms=120000 \
  --producer-property request.timeout.ms=30000
```

```
ACC-001:{"eventType":"fraud.score.low","accountId":"ACC-001","paymentId":"PAY-001","score":12,"timestamp":"2026-09-28T08:00:00.035Z"}
ACC-002:{"eventType":"fraud.score.high","accountId":"ACC-002","paymentId":"PAY-002","score":87,"timestamp":"2026-09-28T08:01:00.038Z"}
```

---

### 3.3 Notification Producer

```bash
kubectl exec -it kafka-0 -- kafka-console-producer \
  --bootstrap-server kafka-service:9092 \
  --topic digital-payment.notification-events \
  --property parse.key=true \
  --property key.separator=: \
  --producer-property acks=all \
  --producer-property retries=10 \
  --producer-property max.in.flight.requests.per.connection=5 \
  --producer-property enable.idempotence=true \
  --producer-property compression.type=snappy \
  --producer-property linger.ms=10 \                        # 2s SLA gives more room to batch
  --producer-property batch.size=16384 \
  --producer-property delivery.timeout.ms=120000 \
  --producer-property request.timeout.ms=30000
```

```
ACC-001:{"eventType":"notification.payment.approved","accountId":"ACC-001","paymentId":"PAY-001","channel":"push","timestamp":"2026-09-28T08:00:00.200Z"}
ACC-002:{"eventType":"notification.payment.failed","accountId":"ACC-002","paymentId":"PAY-002","reason":"insufficient_funds","channel":"push","timestamp":"2026-09-28T08:01:00.150Z"}
```

---

## 4. Consumer Groups

Commands match `payment.md`, `fraud.md`, and `notification.md`.

### 4.1 Audit Consumer

```bash
kubectl exec -it kafka-0 -- kafka-console-consumer \
  --bootstrap-server kafka-service:9092 \
  --topic digital-payment.payment-events \
  --group payment-audit-consumer-group \
  --property parse.key=true \
  --property key.deserializer=org.apache.kafka.common.serialization.StringDeserializer \
  --property value.deserializer=org.apache.kafka.common.serialization.StringDeserializer \
  --consumer-property max.poll.records=100 \                 # high batch size — audit reads all events
  --consumer-property session.timeout.ms=45000 \            # adjusted to meet 50ms fraud SLA
  --consumer-property heartbeat.interval.ms=12000 \         # must be < session.timeout / 3 = 15000
  --consumer-property auto.offset.reset=earliest \          # must read from the start for compliance
  --consumer-property enable.auto.commit=false \            # only mark done after fully processed
  --consumer-property max.poll.interval.ms=300000 \
  --consumer-property fetch.min.bytes=1 \
  --consumer-property fetch.max.wait.ms=500 \
  --consumer-property max.partition.fetch.bytes=1048576 \
  --consumer-property partition.assignment.strategy=org.apache.kafka.clients.consumer.RoundRobinAssignor
```

**Output (all 7 events received):**
```
{"eventType":"payment.initiated","accountId":"ACC-002","paymentId":"PAY-002","amount":50000.00,"currency":"ZAR","channel":"digital","timestamp":"2026-09-28T08:01:00Z"}
{"eventType":"payment.not_authorised","accountId":"ACC-002","paymentId":"PAY-002","reason":"insufficient_funds","timestamp":"2026-09-28T08:01:00.043Z"}
{"eventType":"payment.initiated","accountId":"ACC-001","paymentId":"PAY-001","amount":1500.00,"currency":"ZAR","channel":"digital","timestamp":"2026-09-28T08:00:00Z"}
{"eventType":"payment.initiated","accountId":"ACC-001","paymentId":"PAY-001","amount":1500.00,"currency":"ZAR","channel":"digital","timestamp":"2026-09-28T08:00:00Z"}
{"eventType":"payment.authorised","accountId":"ACC-001","paymentId":"PAY-001","authorisedBy":"auth-engine","timestamp":"2026-09-28T08:00:00.080Z"}
{"eventType":"payment.validated","accountId":"ACC-001","paymentId":"PAY-001","validatedBy":"compliance-engine","timestamp":"2026-09-28T08:00:00.120Z"}
{"eventType":"payment.completed","accountId":"ACC-001","paymentId":"PAY-001","timestamp":"2026-09-28T08:00:00.145Z"}
```

---

### 4.2 Fraud Detection Consumer

```bash
kubectl exec -it kafka-0 -- kafka-console-consumer \
  --bootstrap-server kafka-service:9092 \
  --topic digital-payment.payment-events \
  --group fraud-detection-consumer-group \
  --property parse.key=true \
  --property key.deserializer=org.apache.kafka.common.serialization.StringDeserializer \
  --property value.deserializer=org.apache.kafka.common.serialization.StringDeserializer \
  --consumer-property max.poll.records=10 \                 # small batch — low latency for 50ms SLA
  --consumer-property session.timeout.ms=45000 \
  --consumer-property heartbeat.interval.ms=12000 \
  --consumer-property auto.offset.reset=earliest \          # POC: events already existed before consumer started
  --consumer-property enable.auto.commit=false \
  --consumer-property max.poll.interval.ms=300000 \
  --consumer-property fetch.min.bytes=1 \
  --consumer-property fetch.max.wait.ms=500 \
  --consumer-property max.partition.fetch.bytes=1048576 \
  --consumer-property partition.assignment.strategy=org.apache.kafka.clients.consumer.RoundRobinAssignor
```

In production this consumer would only act on `payment.initiated`. The CLI shows all 7 events because the console consumer can't filter by event type.

**Output (all 7 events received):**
```
{"eventType":"payment.initiated","accountId":"ACC-002","paymentId":"PAY-002","amount":50000.00,"currency":"ZAR","channel":"digital","timestamp":"2026-09-28T08:01:00Z"}
{"eventType":"payment.not_authorised","accountId":"ACC-002","paymentId":"PAY-002","reason":"insufficient_funds","timestamp":"2026-09-28T08:01:00.043Z"}
{"eventType":"payment.initiated","accountId":"ACC-001","paymentId":"PAY-001","amount":1500.00,"currency":"ZAR","channel":"digital","timestamp":"2026-09-28T08:00:00Z"}
{"eventType":"payment.initiated","accountId":"ACC-001","paymentId":"PAY-001","amount":1500.00,"currency":"ZAR","channel":"digital","timestamp":"2026-09-28T08:00:00Z"}
{"eventType":"payment.authorised","accountId":"ACC-001","paymentId":"PAY-001","authorisedBy":"auth-engine","timestamp":"2026-09-28T08:00:00.080Z"}
{"eventType":"payment.validated","accountId":"ACC-001","paymentId":"PAY-001","validatedBy":"compliance-engine","timestamp":"2026-09-28T08:00:00.120Z"}
{"eventType":"payment.completed","accountId":"ACC-001","paymentId":"PAY-001","timestamp":"2026-09-28T08:00:00.145Z"}
```

---

### 4.3 Notification Consumer

```bash
kubectl exec -it kafka-0 -- kafka-console-consumer \
  --bootstrap-server kafka-service:9092 \
  --topic digital-payment.payment-events \
  --group notification-consumer-group \
  --property parse.key=true \
  --property key.deserializer=org.apache.kafka.common.serialization.StringDeserializer \
  --property value.deserializer=org.apache.kafka.common.serialization.StringDeserializer \
  --consumer-property max.poll.records=50 \                 # mid-range batch — 2s SLA
  --consumer-property session.timeout.ms=45000 \
  --consumer-property heartbeat.interval.ms=12000 \
  --consumer-property auto.offset.reset=earliest \          # POC: events already existed before consumer started
  --consumer-property enable.auto.commit=false \
  --consumer-property max.poll.interval.ms=300000 \
  --consumer-property fetch.min.bytes=1 \
  --consumer-property fetch.max.wait.ms=500 \
  --consumer-property max.partition.fetch.bytes=1048576 \
  --consumer-property partition.assignment.strategy=org.apache.kafka.clients.consumer.RoundRobinAssignor
```

In production this consumer would only act on `payment.validated` and `payment.not_completed`. The CLI shows all 7 events because the console consumer can't filter.

**Output (all 7 events received):**
```
{"eventType":"payment.initiated","accountId":"ACC-002","paymentId":"PAY-002","amount":50000.00,"currency":"ZAR","channel":"digital","timestamp":"2026-09-28T08:01:00Z"}
{"eventType":"payment.not_authorised","accountId":"ACC-002","paymentId":"PAY-002","reason":"insufficient_funds","timestamp":"2026-09-28T08:01:00.043Z"}
{"eventType":"payment.initiated","accountId":"ACC-001","paymentId":"PAY-001","amount":1500.00,"currency":"ZAR","channel":"digital","timestamp":"2026-09-28T08:00:00Z"}
{"eventType":"payment.initiated","accountId":"ACC-001","paymentId":"PAY-001","amount":1500.00,"currency":"ZAR","channel":"digital","timestamp":"2026-09-28T08:00:00Z"}
{"eventType":"payment.authorised","accountId":"ACC-001","paymentId":"PAY-001","authorisedBy":"auth-engine","timestamp":"2026-09-28T08:00:00.080Z"}
{"eventType":"payment.validated","accountId":"ACC-001","paymentId":"PAY-001","validatedBy":"compliance-engine","timestamp":"2026-09-28T08:00:00.120Z"}
{"eventType":"payment.completed","accountId":"ACC-001","paymentId":"PAY-001","timestamp":"2026-09-28T08:00:00.145Z"}
```

---

## 5. Verification & Monitoring

### 5.1 Consumer Group Lag

**Lag** = how many messages a consumer group hasn't processed yet. Checked using:

```bash
kubectl exec -it kafka-0 -- kafka-consumer-groups \
  --bootstrap-server kafka-service:9092 \
  --describe --group payment-audit-consumer-group

kubectl exec -it kafka-0 -- kafka-consumer-groups \
  --bootstrap-server kafka-service:9092 \
  --describe --group fraud-detection-consumer-group

kubectl exec -it kafka-0 -- kafka-consumer-groups \
  --bootstrap-server kafka-service:9092 \
  --describe --group notification-consumer-group
```

**payment-audit-consumer-group:**
```
GROUP                        TOPIC                          PARTITION  CURRENT-OFFSET  LOG-END-OFFSET  LAG  CONSUMER-ID                                           HOST          CLIENT-ID
payment-audit-consumer-group digital-payment.payment-events 0          -               5               -    console-consumer-0a426249-0547-488f-bee9-7400134d525c /10.42.0.112  console-consumer
payment-audit-consumer-group digital-payment.payment-events 1          -               0               -    console-consumer-0a426249-0547-488f-bee9-7400134d525c /10.42.0.112  console-consumer
payment-audit-consumer-group digital-payment.payment-events 2          -               0               -    console-consumer-0a426249-0547-488f-bee9-7400134d525c /10.42.0.112  console-consumer
payment-audit-consumer-group digital-payment.payment-events 3          -               0               -    console-consumer-0a426249-0547-488f-bee9-7400134d525c /10.42.0.112  console-consumer
payment-audit-consumer-group digital-payment.payment-events 4          -               2               -    console-consumer-0a426249-0547-488f-bee9-7400134d525c /10.42.0.112  console-consumer
payment-audit-consumer-group digital-payment.payment-events 5          -               0               -    console-consumer-0a426249-0547-488f-bee9-7400134d525c /10.42.0.112  console-consumer
payment-audit-consumer-group digital-payment.payment-events 6          -               0               -    console-consumer-0a426249-0547-488f-bee9-7400134d525c /10.42.0.112  console-consumer
payment-audit-consumer-group digital-payment.payment-events 7          -               0               -    console-consumer-0a426249-0547-488f-bee9-7400134d525c /10.42.0.112  console-consumer
payment-audit-consumer-group digital-payment.payment-events 8          -               0               -    console-consumer-0a426249-0547-488f-bee9-7400134d525c /10.42.0.112  console-consumer
payment-audit-consumer-group digital-payment.payment-events 9          -               0               -    console-consumer-0a426249-0547-488f-bee9-7400134d525c /10.42.0.112  console-consumer
```

**fraud-detection-consumer-group:**
```
GROUP                          TOPIC                          PARTITION  CURRENT-OFFSET  LOG-END-OFFSET  LAG  CONSUMER-ID                                           HOST          CLIENT-ID
fraud-detection-consumer-group digital-payment.payment-events 0          -               5               -    console-consumer-b3c0b4e8-24d7-4bc4-90e9-eaf5267a6607 /10.42.0.112  console-consumer
fraud-detection-consumer-group digital-payment.payment-events 1          -               0               -    console-consumer-b3c0b4e8-24d7-4bc4-90e9-eaf5267a6607 /10.42.0.112  console-consumer
fraud-detection-consumer-group digital-payment.payment-events 2          -               0               -    console-consumer-b3c0b4e8-24d7-4bc4-90e9-eaf5267a6607 /10.42.0.112  console-consumer
fraud-detection-consumer-group digital-payment.payment-events 3          -               0               -    console-consumer-b3c0b4e8-24d7-4bc4-90e9-eaf5267a6607 /10.42.0.112  console-consumer
fraud-detection-consumer-group digital-payment.payment-events 4          -               2               -    console-consumer-b3c0b4e8-24d7-4bc4-90e9-eaf5267a6607 /10.42.0.112  console-consumer
fraud-detection-consumer-group digital-payment.payment-events 5          -               0               -    console-consumer-b3c0b4e8-24d7-4bc4-90e9-eaf5267a6607 /10.42.0.112  console-consumer
fraud-detection-consumer-group digital-payment.payment-events 6          -               0               -    console-consumer-b3c0b4e8-24d7-4bc4-90e9-eaf5267a6607 /10.42.0.112  console-consumer
fraud-detection-consumer-group digital-payment.payment-events 7          -               0               -    console-consumer-b3c0b4e8-24d7-4bc4-90e9-eaf5267a6607 /10.42.0.112  console-consumer
fraud-detection-consumer-group digital-payment.payment-events 8          -               0               -    console-consumer-b3c0b4e8-24d7-4bc4-90e9-eaf5267a6607 /10.42.0.112  console-consumer
fraud-detection-consumer-group digital-payment.payment-events 9          -               0               -    console-consumer-b3c0b4e8-24d7-4bc4-90e9-eaf5267a6607 /10.42.0.112  console-consumer
```

**notification-consumer-group:**
```
GROUP                       TOPIC                          PARTITION  CURRENT-OFFSET  LOG-END-OFFSET  LAG  CONSUMER-ID                                           HOST          CLIENT-ID
notification-consumer-group digital-payment.payment-events 0          -               5               -    console-consumer-767e00c0-9ad1-431e-8b55-a36292bbff4a /10.42.0.112  console-consumer
notification-consumer-group digital-payment.payment-events 1          -               0               -    console-consumer-767e00c0-9ad1-431e-8b55-a36292bbff4a /10.42.0.112  console-consumer
notification-consumer-group digital-payment.payment-events 2          -               0               -    console-consumer-767e00c0-9ad1-431e-8b55-a36292bbff4a /10.42.0.112  console-consumer
notification-consumer-group digital-payment.payment-events 3          -               0               -    console-consumer-767e00c0-9ad1-431e-8b55-a36292bbff4a /10.42.0.112  console-consumer
notification-consumer-group digital-payment.payment-events 4          -               2               -    console-consumer-767e00c0-9ad1-431e-8b55-a36292bbff4a /10.42.0.112  console-consumer
notification-consumer-group digital-payment.payment-events 5          -               0               -    console-consumer-767e00c0-9ad1-431e-8b55-a36292bbff4a /10.42.0.112  console-consumer
notification-consumer-group digital-payment.payment-events 6          -               0               -    console-consumer-767e00c0-9ad1-431e-8b55-a36292bbff4a /10.42.0.112  console-consumer
notification-consumer-group digital-payment.payment-events 7          -               0               -    console-consumer-767e00c0-9ad1-431e-8b55-a36292bbff4a /10.42.0.112  console-consumer
notification-consumer-group digital-payment.payment-events 8          -               0               -    console-consumer-767e00c0-9ad1-431e-8b55-a36292bbff4a /10.42.0.112  console-consumer
notification-consumer-group digital-payment.payment-events 9          -               0               -    console-consumer-767e00c0-9ad1-431e-8b55-a36292bbff4a /10.42.0.112  console-consumer
```

**Why `CURRENT-OFFSET` and `LAG` show `-`:**

Kafka calculates lag as `LOG-END-OFFSET minus CURRENT-OFFSET`. The `CURRENT-OFFSET` (bookmark) only gets saved when the consumer commits its offset. I set `enable.auto.commit=false` so it won't commit automatically, and the CLI tool can't commit manually — that requires application code calling `consumer.commitSync()`. Since no commit happened, Kafka has nothing to show, so both columns display `-`.

This doesn't mean consumers are behind. The output in Section 4 shows all 7 events were received in every consumer terminal. `LOG-END-OFFSET` confirms 7 events exist (5 on partition 0, 2 on partition 4). In a real Java application the output would show `CURRENT-OFFSET: 5` on partition 0, `CURRENT-OFFSET: 2` on partition 4, and `LAG: 0` on all partitions. The `CONSUMER-ID` column confirms each group has an active instance assigned to all 10 partitions.

---

### 5.2 Ordering Verification

Because `accountId` is the partition key, all events for one account always land on the same partition in order:

```
Partition 0, Offset 0: ACC-001 payment.initiated
Partition 0, Offset 1: ACC-001 payment.authorised
Partition 0, Offset 2: ACC-001 payment.validated
Partition 0, Offset 3: ACC-001 payment.completed
```

A consumer reading partition 0 will always see these in this order.

---

## 6. Trade-offs & Justifications

- **Partitions 10 vs 3** — 10 gives consumer parallelism and matches the throughput target (10 × 100 Mb/s = 1000 Mb/s). 3 would bottleneck at 300 Mb/s.
- **Replication 3 vs 2** — 3-broker cluster; RF=3 means zero data loss on a single broker failure.
- **Retention 7yr (payment/fraud) vs 90 days** — regulatory requirement. 90 days fails a compliance audit.
- **Retention 30 days (notification) vs 7yr** — notifications are operational, not compliance. 7 years wastes storage.
- **Cleanup: delete vs compact** — events are immutable facts. Compaction is for state stores. An event stream should never overwrite history.
- **Compression: snappy vs gzip/none** — snappy gives better latency than gzip and better throughput than none for text-heavy JSON.
- **acks=all vs acks=1** — acks=1 risks data loss if the leader dies before replication completes. Not acceptable for payment events.
- **Idempotence enabled vs disabled** — without it, a retry on timeout creates a duplicate payment record.
- **offset.reset=earliest (POC) vs latest (production)** — for fraud and notification, `earliest` was needed to make the demo work since events were produced before consumers started.1
                                                           Production would use `latest` to avoid scoring already-completed payments.
- **offset.reset=earliest (audit)** — audit must be complete from the beginning. `latest` would leave gaps.
- **auto.commit=false vs true** — manual commit prevents silent event loss if the consumer crashes between the auto-commit interval and the last processed event.
- **session.timeout.ms=45000 vs CapitecPay's 90000** — CapitecPay's 90s is valid for their system. Reduced for this project because 90s idle after a consumer crash is unacceptable for a 50ms fraud SLA.
- **heartbeat.interval.ms=12000 vs CapitecPay's 30000** — recalculated after session.timeout changed. Must be strictly less than `session.timeout.ms / 3` (45000 / 3 = 15000), 
                                                          so 12000 was chosen to leave a safe buffer.
- **retries=10 vs CapitecPay's 5** — increased to survive a Kafka leader election (30–60s) and to meet the project's requirement for an explicit, auditable retry count.
- **No size-based retention** — a size cap on payment/fraud topics could delete legally required data before 7 years if volumes grow faster than expected.

---

## 7. Issues Encountered

**Issue 1 — `kafka-topics.sh` not found:** Binary has no `.sh` suffix in this image. Fixed by using `kafka-topics` directly.

**Issue 2 — Rancher Desktop TLS timeout:** `fraud-events` and `notification-events` couldn't be created due to a local Kubernetes connectivity issue. Fixed by restarting Rancher Desktop.

**Issue 3 — `fraud-events` created without full config:** First creation only had `--partitions` and `--replication-factor`, missing retention and compression. Deleted and recreated with all flags, then verified with `--describe`.

**Issue 4 — Three CapitecPay config values needed adjustment for this project's SLAs:** `session.timeout.ms=90000` is valid for CapitecPay but too long for a 50ms fraud SLA. `heartbeat.interval.ms=30000` needed to be recalculated once session.timeout changed. `retries=5` was increased to 10 to meet the project's auditable retry requirement. Adjusted all three.

**Issue 5 — Wrong flag prefix:** Used `--property` for everything. Producer configs need `--producer-property`, consumer configs need `--consumer-property`. Using the wrong one can silently ignore settings like `acks`. Fixed by using the correct prefix per setting type.

**Issue 6 — Fraud and notification consumers missed events:** Both had `auto.offset.reset=latest` — only reads events produced after the consumer starts. Events were already in the topic. Fixed by changing to `earliest` for the POC.

**Issue 7 — Producer run twice, duplicate event:** First session was interrupted after 1 event. Second session sent all 6 again, creating a duplicate `payment.initiated` for ACC-001 (7 events total). Shows that `enable.idempotence=true` only deduplicates within a single session — a real system needs application-level deduplication using `paymentId`.

**Issue 8 — `-it` with `| tee` caused output buffering:** Allocating a TTY while piping buffers output instead of streaming it. Only 1 event appeared before the connection dropped. Fixed by removing `-it` and `| tee`.

---

## 8. Conclusion

This was my first hands-on project with Kafka. I come from a front-end background so working with distributed systems, brokers, consumer groups, and config tuning is still very new to me.
It was challenging but I enjoyed learning about the backend and data engineering side.

**Key things I took away from this project:**
Splitting events into three separate topics made sense once I understood the SLAs. Fraud needs 50ms, notifications need 2s, audit needs completeness — different requirements means they need to be configured and scaled independently.

I used the CapitecPay config as a reference and kept most values the same. Three values were adjusted to meet this project's SLA requirements: session.timeout.ms (90000→45000), heartbeat.interval.ms (30000→12000), and retries (5→10). The comparison gave me some good insight into why reviewing configs against actual requirements matters and why they needed.

`enable.auto.commit=false` made more sense once I better understood what auto-commit actually does — it saves the bookmark after reading, not after processing. Turning it off means you only confirm you're done when you're actually done, which protects against losing events if the consumer crashes.

The duplicate event from restarting the producer was a real example of why idempotency alone isn't enough. It handles retries within a session but not restarts. Application-level deduplication using `paymentId` would be needed in production.


