# Kafka Latency Test Tool (Go)

A Go-based tool for measuring and analyzing Kafka producer and consumer latency with detailed breakdown of individual latency components.

## Overview

This tool generates simulated clickstream events and measures various latency components when producing to and consuming from Kafka. It provides granular timing information for DNS lookups, metadata operations, serialization, network operations, and more.

## Requirements

- Go 1.19+
- Apache Kafka cluster
- IBM Sarama Kafka client library

## Installation

```
go mod tidy
go build -o kafka-latency-test main.go
```

## Usage

### Producer Mode

Produces simulated clickstream events to a Kafka topic and measures producer-side latencies.

```
./kafka-latency-test -mode=producer -brokers=localhost:9092 -topic=clickstream-latency-test -count=100
```

### Consumer Mode

Consumes messages from a Kafka topic and measures consumer-side and end-to-end latencies.

```
./kafka-latency-test -mode=consumer -brokers=localhost:9092 -topic=clickstream-latency-test
```

## Command Line Options

| Option | Default | Description |
|--------|---------|-------------|
| `-mode` | (required) | Operating mode: `producer` or `consumer` |
| `-brokers` | `localhost:9092` | Comma-separated list of Kafka broker addresses |
| `-topic` | `clickstream-latency-test` | Kafka topic name |
| `-count` | `1000` | Number of messages to produce (producer mode only) |
| `-sasl-mechanism` | `PLAIN` | SASL mechanism: `SCRAM-SHA-256` or `SCRAM-SHA-512` |
| `-tls` | `false` | Enable TLS encryption (optional) |
| `-tls-skip-verify` | `false` | Skip TLS certificate verification (optional) |

## TLS Encryption

The tool supports optional TLS encryption for secure communication with Kafka brokers.

### Usage with TLS

```
# Enable TLS
./kafka-latency-test -mode=producer -brokers=broker:9093 -tls

# Enable TLS and skip certificate verification (for self-signed certs)
./kafka-latency-test -mode=producer -brokers=broker:9093 -tls -tls-skip-verify
```

### Notes

TLS uses a minimum version of TLS 1.2. When SASL authentication is enabled without explicitly enabling TLS, TLS is automatically enabled as most Kafka clusters require TLS for SASL authentication.

## Authentication

The tool supports SASL/SCRAM authentication for connecting to secured Kafka clusters. For the `run-test.sh` script, credentials are fetched from AWS Secrets Manager.

### Supported SASL Mechanisms

| Mechanism | Description |
|-----------|-------------|
| `SCRAM-SHA-256` | Salted Challenge Response Authentication with SHA-256 |
| `SCRAM-SHA-512` | Salted Challenge Response Authentication with SHA-512 |

### AWS Secrets Manager Integration

The `run-test.sh` script fetches SCRAM credentials from AWS Secrets Manager. The secret must be a JSON object with `username` and `password` fields:

```json
{
  "username": "admin",
  "password": "your-password"
}
```

### Notes

When authentication is enabled, TLS is automatically enabled with TLS 1.2 minimum version. This is required by most Kafka clusters that use SASL authentication.

## Latency Measurements

### Producer Latency Breakdown

The producer measures the following latency components for each message:

| Metric | Description |
|--------|-------------|
| TotalLatencyMs | Total elapsed time from operation start to completion |
| DNSLookupMs | Time to resolve the broker hostname via DNS |
| MetadataCheckMs | Time to fetch partition metadata from Kafka |
| SerializeMs | Time to generate the clickstream event and serialize to JSON |
| MessagePrepMs | Time to create the Kafka ProducerMessage object with headers |
| NetworkRTTMs | Network Round-Trip Time (TCP Ping) to broker |
| BrokerInternalMs | Estimated broker processing time (Send - RTT) |
| NetworkSendMs | Time for the actual network send operation to Kafka broker |

Console output format:
```
[1/100] Event: page_view    | Total: 5.234ms | DNS: 0.215ms | Meta: 0.018ms | Ser: 0.125ms | Prep: 0.008ms | RTT: 2.000ms | BrkInt: 2.868ms | Send: 4.868ms
```

### Consumer Latency Breakdown

The consumer measures the following latency components for each message:

| Metric | Description |
|--------|-------------|
| TotalLatencyMs | Total latency from producer timestamp to consumer receipt |
| TransitMs | Network and broker transit time (Total - Deserialize - Processing) |
| DeserializeMs | Time to unmarshal the JSON payload |
| ProcessingMs | Time for header parsing and latency calculations |

The latency breakdown follows the formula: **Total = Transit + Deserialize + Processing**

Console output format:
```
[1] P0 | Event: page_view    | Total: 12.000ms | Transit: 11.856ms | Deser: 0.089ms | Proc: 0.055ms
```

## Test Runner Script

The `run-test.sh` script provides an automated way to run both producer and consumer tests with broker connectivity checks.

### Features

- Pre-flight connectivity check for all brokers before starting tests
- Automatic consumer/producer orchestration
- Configurable timeouts and message counts
- Optional SASL authentication support
- Colored output for easy status identification
- Graceful shutdown handling

### Usage

```
./run-test.sh [OPTIONS]
```

### Options

| Option | Long Form | Default | Description |
|--------|-----------|---------|-------------|
| `-b` | `--brokers` | AWS MSK brokers | Comma-separated list of Kafka brokers |
| `-c` | `--count` | 1000 | Number of messages to produce |
| `-t` | `--topic` | clickstream-latency-test | Kafka topic name |
| `-T` | `--timeout` | 10 | Connection timeout in seconds per broker |
| `-w` | `--wait` | 10 | Seconds to wait after producer finishes |
| `-S` | `--secret` | AmazonMSK_Manager | AWS Secrets Manager secret name for SCRAM auth |
| | `--scram` | false | Enable SCRAM authentication using AWS Secrets Manager |
| `-m` | `--mechanism` | PLAIN | SASL mechanism (SCRAM-SHA-256, SCRAM-SHA-512) |
| `-s` | `--tls` | false | Enable TLS encryption |
| `-k` | `--tls-skip-verify` | false | Skip TLS certificate verification |
| `-h` | `--help` | - | Show help message |

### Examples

```
# Run with defaults (AWS MSK brokers, 1000 messages)
./run-test.sh

# Run with local Kafka and 100 messages
./run-test.sh -b localhost:9092 -c 100

# Run with custom brokers, count, and topic
./run-test.sh -b "broker1:9092,broker2:9092" -c 500 -t my-topic

# Run with long-form options
./run-test.sh --brokers localhost:9092 --count 100 --topic test-topic

# Run with SCRAM authentication (uses default secret AmazonMSK_Manager)
./run-test.sh --scram -m SCRAM-SHA-512

# Run with SCRAM authentication using custom secret
./run-test.sh --scram -S MyCustomSecret -m SCRAM-SHA-256

# Run with TLS enabled
./run-test.sh -b broker:9093 -s

# Run with TLS and skip certificate verification
./run-test.sh -b broker:9093 -s -k

# Run with custom timeout (30s per broker) and wait time (20s)
./run-test.sh -T 30 -w 20

# Show help
./run-test.sh --help
```

### Workflow

1. Checks connectivity to each broker using `nc` (netcat)
2. If all brokers are reachable, starts the consumer in background
3. Starts the producer with specified message count
4. Waits for producer to complete
5. Waits additional time for consumer to process remaining messages
6. Gracefully stops the consumer
7. Outputs location of log and summary files

### Exit Codes

| Code | Description |
|------|-------------|
| 0 | Success |
| 1 | Broker connectivity failed or binary not found |

## Output Files

### Directory Structure

```
.
├── log/                          # CSV log files
│   ├── go_producer_latency_YYYYMMDD_HHMMSS.csv
│   └── go_consumer_latency_YYYYMMDD_HHMMSS.csv
├── sum/                          # Summary text files
│   ├── go_producer_latency_YYYYMMDD_HHMMSS_summary.txt
│   └── go_consumer_latency_YYYYMMDD_HHMMSS_summary.txt
├── kafka-latency-test            # Compiled binary
└── run-test.sh                   # Test runner script
```

### Producer CSV Format

```csv
MessageNumber,Timestamp,TotalLatencyMs,DNSLookupMs,MetadataCheckMs,SerializeMs,MessagePrepMs,NetworkRTTMs,BrokerInternalMs,NetworkSendMs,EventType,EventID
1,2026-01-05T16:33:22.320393+03:00,5.234,0.215,0.018,0.125,0.008,2.000,2.868,4.868,page_view,evt_abc123_1234567890
```

| Column | Type | Description |
|--------|------|-------------|
| MessageNumber | int | Sequential message number |
| Timestamp | RFC3339Nano | Timestamp when message was sent |
| TotalLatencyMs | float | Total operation latency in milliseconds |
| DNSLookupMs | float | DNS lookup time in milliseconds |
| MetadataCheckMs | float | Metadata check time in milliseconds |
| SerializeMs | float | Serialization time in milliseconds |
| MessagePrepMs | float | Message preparation time in milliseconds |
| NetworkRTTMs | float | Network Round-Trip Time in milliseconds |
| BrokerInternalMs | float | Estimated broker internal latency in milliseconds |
| NetworkSendMs | float | Network send time in milliseconds |
| EventType | string | Type of clickstream event |
| EventID | string | Unique event identifier |

### Consumer CSV Format

```csv
MessageNumber,Timestamp,TotalLatencyMs,TransitMs,DeserializeMs,ProcessingMs,EventType,EventID,Partition
1,2026-01-05T16:33:22.320393+03:00,12.000,11.856,0.089,0.055,page_view,evt_abc123_1234567890,0
```

| Column | Type | Description |
|--------|------|-------------|
| MessageNumber | int | Sequential message number |
| Timestamp | RFC3339Nano | Timestamp when message was consumed |
| TotalLatencyMs | float | Total latency in milliseconds |
| TransitMs | float | Network and broker transit time in milliseconds |
| DeserializeMs | float | Deserialization time in milliseconds |
| ProcessingMs | float | Processing time in milliseconds |
| EventType | string | Type of clickstream event |
| EventID | string | Unique event identifier |
| Partition | int | Kafka partition ID |

## Summary Statistics

Both producer and consumer modes generate summary statistics upon completion (or Ctrl+C), including client configuration parameters used during the test.

### Producer Summary Example

```
+----------------------------------+------------------+
|     PRODUCER SUMMARY (Go)        |                  |
+----------------------------------+------------------+
| Date                             | 2026-01-05       |
| Time                             | 16:33:22         |
| Brokers                          | localhost:9092   |
| Total Messages Sent              | 100              |
| Total Time                       | 2m30s            |
+----------------------------------+------------------+
|        LATENCY METRICS (ms)      |                  |
+----------------------------------+------------------+
| Min Latency                      |            1.234 |
| Max Latency                      |           25.678 |
| Avg Latency                      |            5.432 |
| P50 Latency (Median)             |            4.567 |
| P90 Latency                      |            8.901 |
| P95 Latency                      |           12.345 |
| P99 Latency                      |           20.123 |
+----------------------------------+------------------+
|     CLIENT CONFIG (Producer)     |                  |
+----------------------------------+------------------+
| RequiredAcks                     | WaitForLocal     |
| Compression                      | None             |
| Flush.Frequency                  | 0 (disabled)     |
| Flush.Messages                   | 1                |
| Flush.Bytes                      | 0                |
| Retry.Max                        | 2                |
| Retry.Backoff                    | 10ms             |
| Timeout                          | 5s               |
| MaxOpenRequests                  | 1                |
| Net.DialTimeout                  | 5s               |
| Net.ReadTimeout                  | 5s               |
| Net.WriteTimeout                 | 5s               |
| Net.KeepAlive                    | 30s              |
| Metadata.RefreshFrequency        | 30m              |
| Metadata.Full                    | false            |
+----------------------------------+------------------+
| CSV Log File                     | log/go_prod...   |
| Summary File                     | sum/go_prod...   |
+----------------------------------+------------------+
```

### Consumer Summary Example

```
+----------------------------------+------------------+
|     CONSUMER SUMMARY (Go)        |                  |
+----------------------------------+------------------+
| Date                             | 2026-01-05       |
| Time                             | 16:35:00         |
| Brokers                          | localhost:9092   |
| Total Messages Consumed          | 100              |
| Total Time                       | 2m35s            |
+----------------------------------+------------------+
|       TOTAL LATENCY (ms)         |                  |
+----------------------------------+------------------+
| Min Total Latency                |           10.234 |
| Max Total Latency                |           45.678 |
| Avg Total Latency                |           18.432 |
| P50 Total Latency (Median)       |           15.567 |
| P90 Total Latency                |           28.901 |
| P95 Total Latency                |           35.345 |
| P99 Total Latency                |           42.123 |
+----------------------------------+------------------+
|     CLIENT CONFIG (Consumer)     |                  |
+----------------------------------+------------------+
| Offsets.Initial                  | OffsetNewest     |
| Fetch.Min                        | 1                |
| Fetch.Default                    | 64KB             |
| Fetch.Max                        | 256KB            |
| MaxWaitTime                      | 10ms             |
| MaxProcessingTime                | 50ms             |
| Retry.Backoff                    | 50ms             |
| ChannelBufferSize                | 64               |
| Net.DialTimeout                  | 5s               |
| Net.ReadTimeout                  | 5s               |
| Net.WriteTimeout                 | 5s               |
| Net.KeepAlive                    | 30s              |
| Metadata.RefreshFrequency        | 30m              |
| Metadata.Full                    | false            |
+----------------------------------+------------------+
| CSV Log File                     | log/go_cons...   |
| Summary File                     | sum/go_cons...   |
+----------------------------------+------------------+
```

## Clickstream Event Schema

The tool generates realistic clickstream events with the following JSON structure:

```json
{
  "event_id": "evt_abc123_1234567890",
  "user_id": "user_xyz789",
  "session_id": "sess_def456ghi",
  "timestamp": 1704470002316,
  "event_type": "page_view",
  "page_url": "https://example.com/products",
  "referrer": "https://google.com",
  "user_agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)...",
  "ip_address": "192.168.1.100",
  "device_type": "desktop",
  "browser": "Chrome",
  "country": "US",
  "city": "New York",
  "product_id": "prod_1234",
  "product_name": "Laptop Pro",
  "price": 999.99,
  "quantity": 1,
  "search_query": "laptop"
}
```

### Event Types

The following event types are randomly generated:
- `page_view` - Page view event
- `click` - Click event (includes product info)
- `scroll` - Scroll event
- `form_submit` - Form submission
- `add_to_cart` - Add to cart (includes product info)
- `purchase` - Purchase event (includes product info)
- `search` - Search event (includes search query)
- `logout` - User logout
- `login` - User login
- `signup` - User signup

## Kafka Configuration

### Producer Settings (Optimized for Low Latency)

| Setting | Value | Description |
|---------|-------|-------------|
| RequiredAcks | WaitForLocal | Only wait for leader acknowledgment |
| Compression | None | No compression for lower latency |
| Flush.Frequency | 0 | Disable time-based batching |
| Flush.Messages | 1 | Send immediately (no batching) |
| Flush.Bytes | 0 | No size-based batching |
| Retry.Max | 2 | Fewer retries for faster failure |
| Retry.Backoff | 10ms | Minimal retry backoff |
| Timeout | 5s | Faster producer timeout |
| Net.KeepAlive | 30s | More frequent keep-alive |
| Net.DialTimeout | 5s | Faster connection timeout |
| Net.ReadTimeout | 5s | Faster read timeout |
| Net.WriteTimeout | 5s | Faster write timeout |
| Net.MaxOpenRequests | 1 | Single in-flight for lower latency |
| Metadata.Full | false | Only fetch metadata for used topics |
| Metadata.RefreshFrequency | 30m | Less frequent metadata refresh |

### Consumer Settings (Optimized for Low Latency)

| Setting | Value | Description |
|---------|-------|-------------|
| Offsets.Initial | OffsetNewest | Start from latest messages |
| Fetch.Min | 1 | Fetch as soon as 1 byte available |
| Fetch.Default | 64KB | Smaller fetch for faster response |
| Fetch.Max | 256KB | Limit max batch size |
| MaxWaitTime | 10ms | **Critical** - minimal broker wait time |
| MaxProcessingTime | 50ms | Faster processing expectation |
| Retry.Backoff | 50ms | Faster retry |
| ChannelBufferSize | 64 | Smaller buffer for lower latency |
| Net.KeepAlive | 30s | More frequent keep-alive |
| Net.DialTimeout | 5s | Faster connection timeout |
| Net.ReadTimeout | 5s | Faster read timeout |
| Net.WriteTimeout | 5s | Faster write timeout |
| Metadata.Full | false | Only fetch metadata for used topics |
| Metadata.RefreshFrequency | 30m | Less frequent metadata refresh |

### AWS MSK Broker Settings (Low Latency)

Recommended MSK configuration for low-latency workloads:

| Setting | Value | Description |
|---------|-------|-------------|
| num.io.threads | 8 | I/O threads for disk operations |
| num.network.threads | 5 | Network threads for request handling |
| num.replica.fetchers | 4 | More parallelism for replication |
| socket.receive.buffer.bytes | 1048576 | 1MB TCP receive buffer |
| socket.send.buffer.bytes | 1048576 | 1MB TCP send buffer |
| replica.lag.time.max.ms | 10000 | Faster replica detection |
| log.flush.interval.messages | 10000 | Less frequent flush (latency vs durability) |
| replica.fetch.max.bytes | 1048576 | 1MB for faster replication |

## Graceful Shutdown

Both producer and consumer modes support graceful shutdown via:
- `Ctrl+C` (SIGINT)
- `SIGTERM`

Upon shutdown, the tool will:
1. Stop producing/consuming messages
2. Print summary statistics to console
3. Save summary to a text file in `sum/` directory
4. Flush and close CSV files

## Examples

### Basic Producer Test

```
# Produce 50 messages to local Kafka
./kafka-latency-test -mode=producer -count=50
```

### Remote Broker Test

```
# Connect to remote Kafka cluster
./kafka-latency-test -mode=producer -brokers=kafka1.example.com:9092,kafka2.example.com:9092 -topic=test-topic -count=100
```

### Consumer with Custom Topic

```
# Consume from a specific topic
./kafka-latency-test -mode=consumer -brokers=localhost:9092 -topic=my-custom-topic
```

### Full Latency Test (Two Terminals)

Terminal 1 (Consumer):
```
./kafka-latency-test -mode=consumer -topic=latency-test
```

Terminal 2 (Producer):
```
./kafka-latency-test -mode=producer -topic=latency-test -count=100
```

## Latency Calculation Notes

### Producer Total Latency

The producer's `TotalLatencyMs` is measured using wall-clock time from the start of the operation to completion. Individual components should approximately sum to the total:

```
TotalLatencyMs ≈ DNSLookupMs + MetadataCheckMs + SerializeMs + MessagePrepMs + NetworkSendMs
NetworkSendMs ≈ NetworkRTTMs + BrokerInternalMs
```

Minor variance may occur due to timing overhead between measurements.

### Consumer Total Latency

The consumer's `TotalLatencyMs` represents the time from when the producer created the message to when the consumer received it:

```
TotalLatencyMs = ConsumerReceiveTime - ProducerSendTime
```

This is calculated using the `produced_at` header timestamp embedded by the producer.

### Consumer Latency Breakdown

The latency is broken down into components that sum to the total:

```
TotalLatencyMs = TransitMs + DeserializeMs + ProcessingMs
```

- **TransitMs**: Network and broker time (calculated as Total - Deserialize - Processing)
- **DeserializeMs**: Time to parse the JSON message
- **ProcessingMs**: Time for header parsing and calculations

This breakdown avoids artificial overhead from extra operations and provides a logical separation.

## Troubleshooting

### Connection Refused

```
Failed to create producer: kafka: client has run out of available brokers
```

Ensure Kafka is running and accessible at the specified broker address.

### Topic Not Found

The tool will auto-create topics if Kafka is configured with `auto.create.topics.enable=true`. Otherwise, create the topic manually:

```
kafka-topics.sh --create --topic clickstream-latency-test --bootstrap-server localhost:9092
```

### High Latency Values

High latency may indicate:
- Network issues between client and broker
- Broker overload
- DNS resolution delays (check DNSLookupMs)
- Slow metadata responses (check MetadataCheckMs)

## License

Internal use only.
