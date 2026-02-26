CQRS and Event Sourcing framework using Java and Apache Kafka.

- Use the mediator pattern to implement command and query dispatchers.
- Create and change the state of an aggregate with event messages.
- Implement an event store / write database in MongoDB.
- Create a read database in MySQL.
- Apply event versioning.
- Implement optimistic concurrency control.
- Produce events to Apache Kafka.
- Consume events from Apache Kafka to populate and alter the read database.
- Replay the event store and recreate the state of the aggregate.
- Separate read and write concerns.
- Structure your code using Domain-Driven-Design best practices.
- Replay the event store to recreate the entire read database.
- Replay the event store to recreate the entire read database into a different database type - PostgreSQL.

## KAFKA

ACK Mode type - ack-mode
| Mode | Khi nào commit? | Use case
|-------------------|-----------------------------------|------------
| RECORD | Sau khi xử lý mỗi record | Default, đơn giản
| BATCH | Sau khi xử lý hết batch | Hiệu năng cao
| TIME | Sau khoảng thời gian | Periodic commit
| COUNT | Sau N records | Fixed count
| COUNT_TIME | COUNT hoặc TIME đến trước | Flexible
| MANUAL | Gọi acknowledge() thủ công | Full control
| MANUAL_IMMEDIATE| Gọi acknowledge() → commit ngay | Your config

```
acks: 0  # Fire and forget (không chờ ACK)
┌──────────┐
│ Producer │ ──Send──> Leader
└──────────┘           (không đợi)
💪 Fastest, ⚠️ Có thể mất data

acks: 1  # Leader ACK (default)
┌──────────┐
│ Producer │ ──Send──> Leader ──ACK──> Producer
└──────────┘           ↓
                   (Replicas đang copy)
⚖️ Balanced

acks: all (hoặc -1)  # All ISR ACK
┌──────────┐
│ Producer │ ──Send──> Leader
└──────────┘             ↓
                     Replicate
                         ↓
                    All ISR ACK
                         ↓
                   ACK to Producer
🛡️ Safest, 🐌 Slowest
```

```
### ✅ Brokers TÁCH BIỆT về:
- Dữ liệu vật lý (mỗi broker có disk riêng)
- Process riêng biệt

### ✅ Brokers KHÔNG TÁCH BIỆT về:
- Metadata (chia sẻ qua Zookeeper)
- Có thể replicate data cho nhau
- Có thể forward requests

### ✅ Partition Distribution:
- Mỗi partition có 1 Leader trên 1 broker
- Leader xử lý ALL read/write
- Follower replicas trên brokers khác
- Clients chỉ connect đến Leader

### ✅ Communication:
- Brokers không truy cập trực tiếp disk của nhau
- Replication qua network (Follower fetch từ Leader)
- Metadata sync qua Zookeeper
- Controller quản lý cluster-wide operations
```

```java
@KafkaListener(topics = "orders", groupId = "bankaccConsumer")
public void consume(
    ConsumerRecord<String, String> record,
    Acknowledgment acknowledgment  // ← Cần inject này
) {
    try {
        // 1. Xử lý message
        processOrder(record.value());

        // 2. Lưu vào database
        orderRepository.save(order);

        // 3. Commit offset NGAY LẬP TỨC
        acknowledgment.acknowledge();  // ← Commit immediately

    } catch (Exception e) {
        // KHÔNG acknowledge → message sẽ được consume lại
        log.error("Failed to process", e);
    }
}
// ✅ Commit ngay lập tức khi gọi acknowledge()
// ✅ Không đợi batch hoặc time interval
// ✅ Full control - bạn quyết định khi nào commit
// ⚠️ Phải handle acknowledgment thủ công

// MANUAL: Commit khi listener method kết thúc hoặc batch/time
// acknowledgment.acknowledge(); // → Queue để commit sau

// MANUAL_IMMEDIATE: Commit ngay
// acknowledgment.acknowledge(); // → Commit NGAY LẬP TỨC
```

### Kafka Producer
### Kafka Consumer
- Consumer do not read directly from the topic, but from a consumer group.
- In a group, if there are 5 consumers, but there are only 3 partitions, only 3 consumers will be able to read from the topic, and the other 2 consumers will be idle.