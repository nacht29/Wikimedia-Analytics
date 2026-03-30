# Title: Kafka UI could not connect because Kafka advertised the wrong address

### Timestamp: 2026-03-23 19:38:26 +08

### Error Summary:
Kafka UI starts inside its own Docker container. It first connects to Kafka using `kafka:9092`, which is the correct internal Docker address. But Kafka was telling clients to switch to `localhost:9092`.

### That is the problem:
- Inside the Kafka UI container, `localhost` means the Kafka UI container itself, not the Kafka broker container.
- So Kafka UI tried to connect to itself on port `9092`, failed, and kept logging connection errors.

### In simple terms:
- `localhost:9092` is only a good address for tools running on the host machine.
- `kafka:9092` is the correct address for other Docker containers.
- Kafka needed to advertise two different entry points, not one shared one.

### Steps Taken To Fix:
1. Checked `kafka-ui.log` and confirmed Kafka UI bootstrapped to `kafka:9092` but then failed on `localhost:9092`.
2. Checked `docker-compose.yml` and found Kafka was advertising `localhost:9092` to all clients.
3. Changed Kafka to use two listeners:
   - Internal Docker listener: `kafka:9092`
   - External host listener: `localhost:29092`
4. Updated the published host port to `29092` so host-side tools have a separate external port.
5. Updated the controller voter address to `kafka:9093` for consistent container-to-container naming.
6. Restarted `kafka` and `kafka-ui`.
7. Verified Kafka now advertises:
   - `INTERNAL://kafka:9092`
   - `EXTERNAL://localhost:29092`
8. Verified fresh Kafka UI logs showed normal cluster metric updates instead of new `localhost:9092` connection failures.

### Result:
- Kafka UI can reach Kafka over Docker networking using `kafka:9092`.
- Host-side clients should now use `localhost:29092`.

#### Self note:
```
so basically, for Kafka instance itselg, export port 29092 to the port 29092 from host machine so that I can interact with the kafka instance in the docker via my laptop, then kafka-ui should connect to the kafka instance container via port 9092
```