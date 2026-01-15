package main

import (
	"crypto/sha256"
	"crypto/sha512"
	"crypto/tls"
	"encoding/csv"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"math/rand"
	"net"
	"os"
	"os/signal"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/IBM/sarama"
	"github.com/xdg-go/scram"
)

// ClickstreamEvent represents a simulated clickstream data payload
type ClickstreamEvent struct {
	EventID     string  `json:"event_id"`
	UserID      string  `json:"user_id"`
	SessionID   string  `json:"session_id"`
	Timestamp   int64   `json:"timestamp"`
	EventType   string  `json:"event_type"`
	PageURL     string  `json:"page_url"`
	Referrer    string  `json:"referrer"`
	UserAgent   string  `json:"user_agent"`
	IPAddress   string  `json:"ip_address"`
	DeviceType  string  `json:"device_type"`
	Browser     string  `json:"browser"`
	Country     string  `json:"country"`
	City        string  `json:"city"`
	ProductID   string  `json:"product_id,omitempty"`
	ProductName string  `json:"product_name,omitempty"`
	Price       float64 `json:"price,omitempty"`
	Quantity    int     `json:"quantity,omitempty"`
	SearchQuery string  `json:"search_query,omitempty"`
}

// Configuration
var (
	kafkaBrokers  = flag.String("brokers", "localhost:9092", "Comma-separated list of Kafka brokers")
	topic         = flag.String("topic", "clickstream-latency-test", "Kafka topic name")
	mode          = flag.String("mode", "", "Mode: 'producer' or 'consumer'")
	messageCount  = flag.Int("count", 1000, "Number of messages to produce")
	username      = flag.String("username", "", "SASL username for authentication (optional)")
	password      = flag.String("password", "", "SASL password for authentication (optional)")
	saslMechanism = flag.String("sasl-mechanism", "PLAIN", "SASL mechanism: PLAIN or SCRAM-SHA-512 (default: PLAIN)")
	tlsEnabled    = flag.Bool("tls", false, "Enable TLS encryption (optional)")
	tlsSkipVerify = flag.Bool("tls-skip-verify", false, "Skip TLS certificate verification (optional)")
)

// Random data pools for generating realistic clickstream events
var (
	eventTypes  = []string{"page_view", "click", "scroll", "form_submit", "add_to_cart", "purchase", "search", "logout", "login", "signup"}
	pages       = []string{"/home", "/products", "/product/123", "/cart", "/checkout", "/profile", "/search", "/about", "/contact", "/blog"}
	referrers   = []string{"https://google.com", "https://facebook.com", "https://twitter.com", "https://instagram.com", "direct", "https://bing.com", "https://youtube.com"}
	userAgents  = []string{"Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36", "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36", "Mozilla/5.0 (iPhone; CPU iPhone OS 14_0 like Mac OS X)", "Mozilla/5.0 (Linux; Android 11; SM-G991B)"}
	deviceTypes = []string{"desktop", "mobile", "tablet"}
	browsers    = []string{"Chrome", "Firefox", "Safari", "Edge", "Opera"}
	countries   = []string{"US", "UK", "DE", "FR", "JP", "CN", "BR", "IN", "AU", "CA"}
	cities      = []string{"New York", "London", "Berlin", "Paris", "Tokyo", "Beijing", "São Paulo", "Mumbai", "Sydney", "Toronto"}
	products    = []string{"Laptop Pro", "Wireless Mouse", "USB-C Hub", "Mechanical Keyboard", "Monitor 27\"", "Webcam HD", "Headphones", "SSD 1TB", "RAM 32GB", "Graphics Card"}
)

func main() {
	flag.Parse()

	if *mode == "" {
		log.Fatal("Mode is required. Use -mode=producer or -mode=consumer")
	}

	switch strings.ToLower(*mode) {
	case "producer":
		runProducer()
	case "consumer":
		runConsumer()
	default:
		log.Fatalf("Invalid mode: %s. Use 'producer' or 'consumer'", *mode)
	}
}

// generateRandomClickstreamEvent creates a realistic clickstream event
func generateRandomClickstreamEvent() ClickstreamEvent {
	eventType := eventTypes[rand.Intn(len(eventTypes))]

	event := ClickstreamEvent{
		EventID:    fmt.Sprintf("evt_%s_%d", randomString(8), time.Now().UnixNano()),
		UserID:     fmt.Sprintf("user_%s", randomString(6)),
		SessionID:  fmt.Sprintf("sess_%s", randomString(10)),
		Timestamp:  time.Now().UnixMilli(),
		EventType:  eventType,
		PageURL:    fmt.Sprintf("https://example.com%s", pages[rand.Intn(len(pages))]),
		Referrer:   referrers[rand.Intn(len(referrers))],
		UserAgent:  userAgents[rand.Intn(len(userAgents))],
		IPAddress:  fmt.Sprintf("%d.%d.%d.%d", rand.Intn(256), rand.Intn(256), rand.Intn(256), rand.Intn(256)),
		DeviceType: deviceTypes[rand.Intn(len(deviceTypes))],
		Browser:    browsers[rand.Intn(len(browsers))],
		Country:    countries[rand.Intn(len(countries))],
		City:       cities[rand.Intn(len(cities))],
	}

	// Add product-specific fields for certain event types
	if eventType == "add_to_cart" || eventType == "purchase" || eventType == "click" {
		event.ProductID = fmt.Sprintf("prod_%d", rand.Intn(10000))
		event.ProductName = products[rand.Intn(len(products))]
		event.Price = float64(rand.Intn(100000)) / 100.0
		event.Quantity = rand.Intn(5) + 1
	}

	if eventType == "search" {
		searchTerms := []string{"laptop", "wireless mouse", "keyboard", "monitor", "headphones", "usb hub", "webcam"}
		event.SearchQuery = searchTerms[rand.Intn(len(searchTerms))]
	}

	return event
}

func randomString(length int) string {
	const charset = "abcdefghijklmnopqrstuvwxyz0123456789"
	b := make([]byte, length)
	for i := range b {
		b[i] = charset[rand.Intn(len(charset))]
	}
	return string(b)
}

// SCRAM client implementation for SASL authentication
var (
	SHA256 scram.HashGeneratorFcn = sha256.New
	SHA512 scram.HashGeneratorFcn = sha512.New
)

type XDGSCRAMClient struct {
	*scram.Client
	*scram.ClientConversation
	scram.HashGeneratorFcn
}

func (x *XDGSCRAMClient) Begin(userName, password, authzID string) (err error) {
	x.Client, err = x.HashGeneratorFcn.NewClient(userName, password, authzID)
	if err != nil {
		return err
	}
	x.ClientConversation = x.Client.NewConversation()
	return nil
}

func (x *XDGSCRAMClient) Step(challenge string) (response string, err error) {
	response, err = x.ClientConversation.Step(challenge)
	return
}

func (x *XDGSCRAMClient) Done() bool {
	return x.ClientConversation.Done()
}

// applyTLSConfig applies TLS settings to a Sarama config
func applyTLSConfig(config *sarama.Config) {
	if *tlsEnabled {
		config.Net.TLS.Enable = true
		config.Net.TLS.Config = &tls.Config{
			InsecureSkipVerify: *tlsSkipVerify,
			MinVersion:         tls.VersionTLS12,
		}
	}
}

// applySASLConfig applies SASL authentication settings to a Sarama config
func applySASLConfig(config *sarama.Config) {
	if *username != "" && *password != "" {
		config.Net.SASL.Enable = true
		config.Net.SASL.User = *username
		config.Net.SASL.Password = *password
		config.Net.SASL.Handshake = true

		switch strings.ToUpper(*saslMechanism) {
		case "SCRAM-SHA-512":
			config.Net.SASL.SCRAMClientGeneratorFunc = func() sarama.SCRAMClient {
				return &XDGSCRAMClient{HashGeneratorFcn: SHA512}
			}
			config.Net.SASL.Mechanism = sarama.SASLTypeSCRAMSHA512
		case "SCRAM-SHA-256":
			config.Net.SASL.SCRAMClientGeneratorFunc = func() sarama.SCRAMClient {
				return &XDGSCRAMClient{HashGeneratorFcn: SHA256}
			}
			config.Net.SASL.Mechanism = sarama.SASLTypeSCRAMSHA256
		default:
			config.Net.SASL.Mechanism = sarama.SASLTypePlaintext
		}

		// Enable TLS when using SASL (typically required) unless already configured
		if !config.Net.TLS.Enable {
			config.Net.TLS.Enable = true
			config.Net.TLS.Config = &tls.Config{
				InsecureSkipVerify: *tlsSkipVerify,
				MinVersion:         tls.VersionTLS12,
			}
		}
	}
}

// getOptimizedProducerConfig returns a Sarama config optimized for low latency
func getOptimizedProducerConfig() *sarama.Config {
	config := sarama.NewConfig()

	// Producer settings - aggressive low latency
	config.Producer.Return.Successes = true
	config.Producer.Return.Errors = true
	config.Producer.RequiredAcks = sarama.WaitForLocal   // ack from leader only (faster)
	config.Producer.Compression = sarama.CompressionNone // no compression for lower latency
	config.Producer.Flush.Frequency = 0                  // disable time-based batching
	config.Producer.Flush.Messages = 1                   // send immediately
	config.Producer.Flush.MaxMessages = 1                // no batching
	config.Producer.Flush.Bytes = 0                      // no size-based batching
	config.Producer.Retry.Max = 2                        // fewer retries for faster failure
	config.Producer.Retry.Backoff = 10 * time.Millisecond // minimal retry backoff
	config.Producer.Timeout = 5 * time.Second            // faster timeout

	// Network optimizations - aggressive timeouts
	config.Net.KeepAlive = 30 * time.Second  // more frequent keep-alive
	config.Net.DialTimeout = 5 * time.Second // faster connection timeout
	config.Net.ReadTimeout = 5 * time.Second // faster read timeout
	config.Net.WriteTimeout = 5 * time.Second // faster write timeout
	config.Net.MaxOpenRequests = 1           // single in-flight for ordering & lower latency

	// Default: Plaintext connection - no TLS encryption
	config.Net.TLS.Enable = false

	// Default: No authentication
	config.Net.SASL.Enable = false

	// Apply TLS if enabled
	applyTLSConfig(config)

	// Apply SASL authentication if credentials provided
	applySASLConfig(config)

	// Metadata settings - cache longer to avoid overhead
	config.Metadata.Retry.Max = 2
	config.Metadata.Retry.Backoff = 100 * time.Millisecond
	config.Metadata.RefreshFrequency = 30 * time.Minute // less frequent refresh
	config.Metadata.Full = false                        // only fetch metadata for used topics

	// Client ID
	config.ClientID = "kafka-latency-producer"

	// Version (adjust based on your Kafka version)
	config.Version = sarama.V2_8_0_0

	return config
}

// getOptimizedConsumerConfig returns a Sarama config optimized for low latency consumption
func getOptimizedConsumerConfig() *sarama.Config {
	config := sarama.NewConfig()

	// Consumer settings - aggressive low latency
	config.Consumer.Return.Errors = true
	config.Consumer.Offsets.Initial = sarama.OffsetNewest // start from latest
	config.Consumer.Fetch.Min = 1                         // fetch as soon as 1 byte available
	config.Consumer.Fetch.Default = 64 * 1024             // 64KB - smaller fetch for faster response
	config.Consumer.Fetch.Max = 256 * 1024                // 256KB max - limit batch size
	config.Consumer.MaxWaitTime = 10 * time.Millisecond   // minimal wait - key for low latency!
	config.Consumer.MaxProcessingTime = 50 * time.Millisecond // faster processing expectation
	config.Consumer.Retry.Backoff = 50 * time.Millisecond // faster retry
	config.Consumer.Group.Session.Timeout = 10 * time.Second
	config.Consumer.Group.Heartbeat.Interval = 3 * time.Second

	// Channel buffer - smaller for lower latency
	config.ChannelBufferSize = 64 // reduced from default 256

	// Network optimizations - aggressive timeouts
	config.Net.KeepAlive = 30 * time.Second  // more frequent keep-alive
	config.Net.DialTimeout = 5 * time.Second // faster connection
	config.Net.ReadTimeout = 5 * time.Second // faster read timeout
	config.Net.WriteTimeout = 5 * time.Second // faster write timeout

	// Default: Plaintext connection - no TLS encryption
	config.Net.TLS.Enable = false

	// Default: No authentication
	config.Net.SASL.Enable = false

	// Apply TLS if enabled
	applyTLSConfig(config)

	// Apply SASL authentication if credentials provided
	applySASLConfig(config)

	// Metadata settings - cache longer to avoid overhead
	config.Metadata.Retry.Max = 2
	config.Metadata.Retry.Backoff = 100 * time.Millisecond
	config.Metadata.RefreshFrequency = 30 * time.Minute // less frequent refresh
	config.Metadata.Full = false                        // only fetch metadata for used topics

	// Client ID
	config.ClientID = "kafka-latency-consumer"

	// Version
	config.Version = sarama.V2_8_0_0

	return config
}

// runProducer runs the producer mode
func runProducer() {
	brokers := strings.Split(*kafkaBrokers, ",")
	config := getOptimizedProducerConfig()

	// Create client first to access metadata metrics
	client, err := sarama.NewClient(brokers, config)
	if err != nil {
		log.Fatalf("Failed to create client: %v", err)
	}
	defer client.Close()

	// Create producer from client
	producer, err := sarama.NewSyncProducerFromClient(client)
	if err != nil {
		log.Fatalf("Failed to create producer: %v", err)
	}
	defer producer.Close()

	// Ensure log directory exists
	os.MkdirAll("log", 0755)

	// Create CSV file with timestamp in log directory
	timestamp := time.Now().Format("20060102_150405")
	csvFilename := filepath.Join("log", fmt.Sprintf("go_producer_latency_%s.csv", timestamp))
	csvFile, err := os.Create(csvFilename)
	if err != nil {
		log.Fatalf("Failed to create CSV file: %v", err)
	}
	defer csvFile.Close()

	csvWriter := csv.NewWriter(csvFile)
	defer csvWriter.Flush()

	// Write CSV header with detailed latency breakdown
	csvWriter.Write([]string{
		"MessageNumber", "Timestamp", "TotalLatencyMs",
		"DNSLookupMs", "MetadataCheckMs", "SerializeMs", "MessagePrepMs", 
		"NetworkRTTMs", "BrokerInternalMs", "NetworkSendMs", // Added broken down metrics
		"EventType", "EventID",
	})

	// Latency tracking
	var latencies []float64
	var totalLatency float64

	fmt.Printf("=== KAFKA PRODUCER STARTED (Go) ===\n")
	fmt.Printf("Date/Time: %s\n", time.Now().Format("2006-01-02 15:04:05"))
	fmt.Printf("Brokers: %s\n", *kafkaBrokers)
	fmt.Printf("Topic: %s\n", *topic)
	fmt.Printf("Messages to produce: %d\n", *messageCount)
	fmt.Printf("CSV Output: %s\n", csvFilename)
	fmt.Printf("===================================\n\n")

	// Handle graceful shutdown
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	startTime := time.Now()

	// Get first broker host and port for measurements
	firstBroker := strings.Split(*kafkaBrokers, ",")[0]
	brokerHost, brokerPort, err := net.SplitHostPort(firstBroker)
	if err != nil {
		// Fallback if no port specified
		brokerHost = firstBroker
		brokerPort = "9092"
	}

	for i := 1; i <= *messageCount; i++ {
		select {
		case <-sigChan:
			fmt.Println("\nShutdown signal received, stopping producer...")
			printProducerSummary(latencies, totalLatency, i-1, startTime, csvFilename, *kafkaBrokers)
			return
		default:
		}

		// Start total operation timer
		opStart := time.Now()

		// 1. DNS Lookup Measurement
		dnsStart := time.Now()
		net.LookupHost(brokerHost)
		dnsMs := float64(time.Since(dnsStart).Microseconds()) / 1000.0

		// 2. Metadata Check Measurement (Explicit refresh)
		metaStart := time.Now()
		// Use the client we created to fetch partitions, forcing a metadata access check
		client.Partitions(*topic)
		metaMs := float64(time.Since(metaStart).Microseconds()) / 1000.0

		// 3. Serialize Measurement (JSON marshal)
		serializeStart := time.Now()
		event := generateRandomClickstreamEvent()
		eventJSON, err := json.Marshal(event)
		if err != nil {
			log.Printf("Failed to marshal event: %v", err)
			continue
		}
		serializeMs := float64(time.Since(serializeStart).Microseconds()) / 1000.0

		// 4. Message Preparation Measurement
		prepStart := time.Now()
		msg := &sarama.ProducerMessage{
			Topic: *topic,
			Value: sarama.ByteEncoder(eventJSON),
			Headers: []sarama.RecordHeader{
				{Key: []byte("produced_at"), Value: []byte(fmt.Sprintf("%d", time.Now().UnixMilli()))},
			},
		}
		prepMs := float64(time.Since(prepStart).Microseconds()) / 1000.0

		// 5. Network RTT Measurement (TCP Ping)
		rttStart := time.Now()
		conn, err := net.DialTimeout("tcp", net.JoinHostPort(brokerHost, brokerPort), 2*time.Second)
		if err == nil {
			conn.Close()
		}
		rttMs := float64(time.Since(rttStart).Microseconds()) / 1000.0

		// 6. Network Send Measurement (actual send to Kafka)
		sendStart := time.Now()
		_, _, err = producer.SendMessage(msg)
		sendMs := float64(time.Since(sendStart).Microseconds()) / 1000.0
		
		// Calculate Broker Internal Latency (Send - RTT)
		// This is an approximation. If RTT is larger than Send (unlikely but possible due to noise), clamp to 0.
		brokerInternalMs := sendMs - rttMs
		if brokerInternalMs < 0 {
			brokerInternalMs = 0
		}

		if err != nil {
			log.Printf("Failed to send message %d: %v", i, err)
			continue
		}

		// Calculate total latency (sum of all measured components)
		totalMs := float64(time.Since(opStart).Microseconds()) / 1000.0

		latencies = append(latencies, totalMs)
		totalLatency += totalMs

		// Console output with detailed breakdown
		fmt.Printf("[%d/%d] Event: %-12s | Total: %6.3fms | DNS: %5.3fms | Meta: %5.3fms | Ser: %5.3fms | Prep: %5.3fms | RTT: %5.3fms | BrkInt: %5.3fms | Send: %6.3fms\n",
			i, *messageCount, event.EventType, totalMs, dnsMs, metaMs, serializeMs, prepMs, rttMs, brokerInternalMs, sendMs)

		// Write to CSV with detailed breakdown
		csvWriter.Write([]string{
			strconv.Itoa(i),
			time.Now().Format(time.RFC3339Nano),
			fmt.Sprintf("%.3f", totalMs),
			fmt.Sprintf("%.3f", dnsMs),
			fmt.Sprintf("%.3f", metaMs),
			fmt.Sprintf("%.3f", serializeMs),
			fmt.Sprintf("%.3f", prepMs),
			fmt.Sprintf("%.3f", rttMs),
			fmt.Sprintf("%.3f", brokerInternalMs),
			fmt.Sprintf("%.3f", sendMs),
			event.EventType,
			event.EventID,
		})
		csvWriter.Flush()

		// Random delay between 1.0 and 2.0 seconds
		if i < *messageCount {
			delay := 1.0 + rand.Float64() // 1.0 to 2.0 seconds
			time.Sleep(time.Duration(delay * float64(time.Second)))
		}
	}

	printProducerSummary(latencies, totalLatency, *messageCount, startTime, csvFilename, *kafkaBrokers)
}

func printProducerSummary(latencies []float64, totalLatency float64, count int, startTime time.Time, csvFilename string, brokers string) {
	if count == 0 {
		fmt.Println("\nNo messages were sent.")
		return
	}

	// Sort latencies for percentile calculation
	sorted := make([]float64, len(latencies))
	copy(sorted, latencies)
	sort.Float64s(sorted)

	avgLatency := totalLatency / float64(count)
	minLatency := sorted[0]
	maxLatency := sorted[len(sorted)-1]
	p50 := percentile(sorted, 50)
	p90 := percentile(sorted, 90)
	p95 := percentile(sorted, 95)
	p99 := percentile(sorted, 99)

	elapsed := time.Since(startTime)

	currentTime := time.Now()
	dateStr := currentTime.Format("2006-01-02")
	timeStr := currentTime.Format("15:04:05")

	// Ensure sum directory exists
	os.MkdirAll("sum", 0755)

	// Extract base filename and change path to sum directory
	baseFilename := filepath.Base(csvFilename)
	txtFilename := filepath.Join("sum", strings.Replace(baseFilename, ".csv", "_summary.txt", 1))

	// Build table output
	var tableBuilder strings.Builder
	tableBuilder.WriteString("\n")
	tableBuilder.WriteString("+----------------------------------+------------------+\n")
	tableBuilder.WriteString("|     PRODUCER SUMMARY (Go)        |                  |\n")
	tableBuilder.WriteString("+----------------------------------+------------------+\n")
	tableBuilder.WriteString(fmt.Sprintf("| %-32s | %-16s |\n", "Date", dateStr))
	tableBuilder.WriteString(fmt.Sprintf("| %-32s | %-16s |\n", "Time", timeStr))
	tableBuilder.WriteString(fmt.Sprintf("| %-32s | %-16s |\n", "Brokers", brokers))
	tableBuilder.WriteString(fmt.Sprintf("| %-32s | %-16d |\n", "Total Messages Sent", count))
	tableBuilder.WriteString(fmt.Sprintf("| %-32s | %-16s |\n", "Total Time", elapsed.Round(time.Millisecond)))
	tableBuilder.WriteString("+----------------------------------+------------------+\n")
	tableBuilder.WriteString("|        LATENCY METRICS (ms)      |                  |\n")
	tableBuilder.WriteString("+----------------------------------+------------------+\n")
	tableBuilder.WriteString(fmt.Sprintf("| %-32s | %16.3f |\n", "Min Latency", minLatency))
	tableBuilder.WriteString(fmt.Sprintf("| %-32s | %16.3f |\n", "Max Latency", maxLatency))
	tableBuilder.WriteString(fmt.Sprintf("| %-32s | %16.3f |\n", "Avg Latency", avgLatency))
	tableBuilder.WriteString(fmt.Sprintf("| %-32s | %16.3f |\n", "P50 Latency (Median)", p50))
	tableBuilder.WriteString(fmt.Sprintf("| %-32s | %16.3f |\n", "P90 Latency", p90))
	tableBuilder.WriteString(fmt.Sprintf("| %-32s | %16.3f |\n", "P95 Latency", p95))
	tableBuilder.WriteString(fmt.Sprintf("| %-32s | %16.3f |\n", "P99 Latency", p99))
	tableBuilder.WriteString("+----------------------------------+------------------+\n")
	tableBuilder.WriteString("|     CLIENT CONFIG (Producer)     |                  |\n")
	tableBuilder.WriteString("+----------------------------------+------------------+\n")
	tableBuilder.WriteString(fmt.Sprintf("| %-32s | %-16s |\n", "RequiredAcks", "WaitForLocal"))
	tableBuilder.WriteString(fmt.Sprintf("| %-32s | %-16s |\n", "Compression", "None"))
	tableBuilder.WriteString(fmt.Sprintf("| %-32s | %-16s |\n", "Flush.Frequency", "0 (disabled)"))
	tableBuilder.WriteString(fmt.Sprintf("| %-32s | %-16d |\n", "Flush.Messages", 1))
	tableBuilder.WriteString(fmt.Sprintf("| %-32s | %-16d |\n", "Flush.Bytes", 0))
	tableBuilder.WriteString(fmt.Sprintf("| %-32s | %-16d |\n", "Retry.Max", 2))
	tableBuilder.WriteString(fmt.Sprintf("| %-32s | %-16s |\n", "Retry.Backoff", "10ms"))
	tableBuilder.WriteString(fmt.Sprintf("| %-32s | %-16s |\n", "Timeout", "5s"))
	tableBuilder.WriteString(fmt.Sprintf("| %-32s | %-16d |\n", "MaxOpenRequests", 1))
	tableBuilder.WriteString(fmt.Sprintf("| %-32s | %-16s |\n", "Net.DialTimeout", "5s"))
	tableBuilder.WriteString(fmt.Sprintf("| %-32s | %-16s |\n", "Net.ReadTimeout", "5s"))
	tableBuilder.WriteString(fmt.Sprintf("| %-32s | %-16s |\n", "Net.WriteTimeout", "5s"))
	tableBuilder.WriteString(fmt.Sprintf("| %-32s | %-16s |\n", "Net.KeepAlive", "30s"))
	tableBuilder.WriteString(fmt.Sprintf("| %-32s | %-16s |\n", "Metadata.RefreshFrequency", "30m"))
	tableBuilder.WriteString(fmt.Sprintf("| %-32s | %-16t |\n", "Metadata.Full", false))
	tableBuilder.WriteString("+----------------------------------+------------------+\n")
	tableBuilder.WriteString(fmt.Sprintf("| %-32s | %-16s |\n", "CSV Log File", csvFilename))
	tableBuilder.WriteString(fmt.Sprintf("| %-32s | %-16s |\n", "Summary File", txtFilename))
	tableBuilder.WriteString("+----------------------------------+------------------+\n")

	tableOutput := tableBuilder.String()

	// Print to console
	fmt.Print(tableOutput)

	// Write to txt file
	err := os.WriteFile(txtFilename, []byte(tableOutput), 0644)
	if err != nil {
		log.Printf("Failed to write summary file: %v", err)
	} else {
		fmt.Printf("\nSummary saved to: %s\n", txtFilename)
	}
}

// runConsumer runs the consumer mode
func runConsumer() {
	brokers := strings.Split(*kafkaBrokers, ",")
	config := getOptimizedConsumerConfig()

	// Create consumer
	consumer, err := sarama.NewConsumer(brokers, config)
	if err != nil {
		log.Fatalf("Failed to create consumer: %v", err)
	}
	defer consumer.Close()

	// Get partition list
	partitions, err := consumer.Partitions(*topic)
	if err != nil {
		log.Fatalf("Failed to get partitions: %v", err)
	}

	// Ensure log directory exists
	os.MkdirAll("log", 0755)

	// Create CSV file with timestamp in log directory
	timestamp := time.Now().Format("20060102_150405")
	csvFilename := filepath.Join("log", fmt.Sprintf("go_consumer_latency_%s.csv", timestamp))
	csvFile, err := os.Create(csvFilename)
	if err != nil {
		log.Fatalf("Failed to create CSV file: %v", err)
	}
	defer csvFile.Close()

	csvWriter := csv.NewWriter(csvFile)
	defer csvWriter.Flush()

	// Write CSV header with detailed latency breakdown
	// Total = Transit + Deserialize + Processing
	csvWriter.Write([]string{
		"MessageNumber", "Timestamp", "TotalLatencyMs",
		"TransitMs", "DeserializeMs", "ProcessingMs",
		"EventType", "EventID", "Partition",
	})

	// Latency tracking
	var latencies []float64
	var totalLatency float64
	var mu sync.Mutex
	var csvMu sync.Mutex // Mutex for CSV writing
	messageCount := 0

	fmt.Printf("=== KAFKA CONSUMER STARTED (Go) ===\n")
	fmt.Printf("Date/Time: %s\n", time.Now().Format("2006-01-02 15:04:05"))
	fmt.Printf("Brokers: %s\n", *kafkaBrokers)
	fmt.Printf("Topic: %s\n", *topic)
	fmt.Printf("Partitions: %v\n", partitions)
	fmt.Printf("CSV Output: %s\n", csvFilename)
	fmt.Printf("Waiting for messages... (Ctrl+C to stop)\n")
	fmt.Printf("====================================\n\n")

	// Handle graceful shutdown
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	startTime := time.Now()

	var wg sync.WaitGroup

	// Function to consume from a partition
	consumePartition := func(partition int32) {
		defer wg.Done()

		partitionConsumer, err := consumer.ConsumePartition(*topic, partition, sarama.OffsetNewest)
		if err != nil {
			log.Printf("Failed to create partition consumer for partition %d: %v", partition, err)
			return
		}
		defer partitionConsumer.Close()

		for {
			select {
			// case <-sigChan:
			// 	return
			
			case err := <-partitionConsumer.Errors():
				log.Printf("Consumer error on partition %d: %v", partition, err)

		case msg := <-partitionConsumer.Messages():
				// Record consumed timestamp immediately
				consumedAt := time.Now()

				// 1. Deserialize Measurement (JSON unmarshal)
				deserStart := time.Now()
				var event ClickstreamEvent
				json.Unmarshal(msg.Value, &event)
				deserMs := float64(time.Since(deserStart).Microseconds()) / 1000.0

				// 2. Processing time (header parsing and latency calculation)
				procStart := time.Now()
				
				mu.Lock()
				messageCount++
				currentCount := messageCount
				mu.Unlock()

				// Calculate total latency from producer timestamp
				var totalLatencyMs float64
				for _, header := range msg.Headers {
					if string(header.Key) == "produced_at" {
						producedAtMs, _ := strconv.ParseInt(string(header.Value), 10, 64)
						totalLatencyMs = float64(consumedAt.UnixMilli() - producedAtMs)
						break
					}
				}

				if totalLatencyMs == 0 && event.Timestamp > 0 {
					totalLatencyMs = float64(consumedAt.UnixMilli() - event.Timestamp)
				}
				procMs := float64(time.Since(procStart).Microseconds()) / 1000.0

				// Calculate transit latency (network + broker time)
				// Total = Transit + Deserialize + Processing
				transitMs := totalLatencyMs - deserMs - procMs
				if transitMs < 0 {
					transitMs = 0
				}

				mu.Lock()
				latencies = append(latencies, totalLatencyMs)
				totalLatency += totalLatencyMs
				mu.Unlock()

				// Console output
				fmt.Printf("[%d] P%d | Event: %-12s | Total: %8.3fms | Transit: %8.3fms | Deser: %5.3fms | Proc: %5.3fms\n",
					currentCount, partition, event.EventType, totalLatencyMs, transitMs, deserMs, procMs)

				// Write to CSV
				csvMu.Lock()
				csvWriter.Write([]string{
					strconv.Itoa(currentCount),
					consumedAt.Format(time.RFC3339Nano),
					fmt.Sprintf("%.3f", totalLatencyMs),
					fmt.Sprintf("%.3f", transitMs),
					fmt.Sprintf("%.3f", deserMs),
					fmt.Sprintf("%.3f", procMs),
					event.EventType,
					event.EventID,
					fmt.Sprintf("%d", partition),
				})
				csvWriter.Flush()
				csvMu.Unlock()
			}
		}
	}

	// Start a consumer for each partition
	for _, partition := range partitions {
		wg.Add(1)
		go consumePartition(partition)
	}

	// Wait for shutdown signal (handled inside consumePartition via sigChan check, 
	// but we need to block main thread until then)
	<-sigChan
	fmt.Println("\nShutdown signal received, stopping consumer...")
	// Allow some time for goroutines to exit if needed, though they check sigChan
	// We can just print summary and exit.
	
	printConsumerSummary(latencies, totalLatency, messageCount, startTime, csvFilename, *kafkaBrokers)
}

func printConsumerSummary(latencies []float64, totalLatency float64, count int, startTime time.Time, csvFilename string, brokers string) {
	if count == 0 {
		fmt.Println("\nNo messages were consumed.")
		return
	}

	// Sort latencies for percentile calculation
	sorted := make([]float64, len(latencies))
	copy(sorted, latencies)
	sort.Float64s(sorted)

	avgLatency := totalLatency / float64(count)
	minLatency := sorted[0]
	maxLatency := sorted[len(sorted)-1]
	p50 := percentile(sorted, 50)
	p90 := percentile(sorted, 90)
	p95 := percentile(sorted, 95)
	p99 := percentile(sorted, 99)

	elapsed := time.Since(startTime)

	currentTime := time.Now()
	dateStr := currentTime.Format("2006-01-02")
	timeStr := currentTime.Format("15:04:05")

	// Ensure sum directory exists
	os.MkdirAll("sum", 0755)

	// Extract base filename and change path to sum directory
	baseFilename := filepath.Base(csvFilename)
	txtFilename := filepath.Join("sum", strings.Replace(baseFilename, ".csv", "_summary.txt", 1))

	// Build table output
	var tableBuilder strings.Builder
	tableBuilder.WriteString("\n")
	tableBuilder.WriteString("+----------------------------------+------------------+\n")
	tableBuilder.WriteString("|     CONSUMER SUMMARY (Go)        |                  |\n")
	tableBuilder.WriteString("+----------------------------------+------------------+\n")
	tableBuilder.WriteString(fmt.Sprintf("| %-32s | %-16s |\n", "Date", dateStr))
	tableBuilder.WriteString(fmt.Sprintf("| %-32s | %-16s |\n", "Time", timeStr))
	tableBuilder.WriteString(fmt.Sprintf("| %-32s | %-16s |\n", "Brokers", brokers))
	tableBuilder.WriteString(fmt.Sprintf("| %-32s | %-16d |\n", "Total Messages Consumed", count))
	tableBuilder.WriteString(fmt.Sprintf("| %-32s | %-16s |\n", "Total Time", elapsed.Round(time.Millisecond)))
	tableBuilder.WriteString("+----------------------------------+------------------+\n")
	tableBuilder.WriteString("|       TOTAL LATENCY (ms)         |                  |\n")
	tableBuilder.WriteString("+----------------------------------+------------------+\n")
	tableBuilder.WriteString(fmt.Sprintf("| %-32s | %16.3f |\n", "Min Total Latency", minLatency))
	tableBuilder.WriteString(fmt.Sprintf("| %-32s | %16.3f |\n", "Max Total Latency", maxLatency))
	tableBuilder.WriteString(fmt.Sprintf("| %-32s | %16.3f |\n", "Avg Total Latency", avgLatency))
	tableBuilder.WriteString(fmt.Sprintf("| %-32s | %16.3f |\n", "P50 Total Latency (Median)", p50))
	tableBuilder.WriteString(fmt.Sprintf("| %-32s | %16.3f |\n", "P90 Total Latency", p90))
	tableBuilder.WriteString(fmt.Sprintf("| %-32s | %16.3f |\n", "P95 Total Latency", p95))
	tableBuilder.WriteString(fmt.Sprintf("| %-32s | %16.3f |\n", "P99 Total Latency", p99))
	tableBuilder.WriteString("+----------------------------------+------------------+\n")
	tableBuilder.WriteString("|     CLIENT CONFIG (Consumer)     |                  |\n")
	tableBuilder.WriteString("+----------------------------------+------------------+\n")
	tableBuilder.WriteString(fmt.Sprintf("| %-32s | %-16s |\n", "Offsets.Initial", "OffsetNewest"))
	tableBuilder.WriteString(fmt.Sprintf("| %-32s | %-16d |\n", "Fetch.Min", 1))
	tableBuilder.WriteString(fmt.Sprintf("| %-32s | %-16s |\n", "Fetch.Default", "64KB"))
	tableBuilder.WriteString(fmt.Sprintf("| %-32s | %-16s |\n", "Fetch.Max", "256KB"))
	tableBuilder.WriteString(fmt.Sprintf("| %-32s | %-16s |\n", "MaxWaitTime", "10ms"))
	tableBuilder.WriteString(fmt.Sprintf("| %-32s | %-16s |\n", "MaxProcessingTime", "50ms"))
	tableBuilder.WriteString(fmt.Sprintf("| %-32s | %-16s |\n", "Retry.Backoff", "50ms"))
	tableBuilder.WriteString(fmt.Sprintf("| %-32s | %-16d |\n", "ChannelBufferSize", 64))
	tableBuilder.WriteString(fmt.Sprintf("| %-32s | %-16s |\n", "Net.DialTimeout", "5s"))
	tableBuilder.WriteString(fmt.Sprintf("| %-32s | %-16s |\n", "Net.ReadTimeout", "5s"))
	tableBuilder.WriteString(fmt.Sprintf("| %-32s | %-16s |\n", "Net.WriteTimeout", "5s"))
	tableBuilder.WriteString(fmt.Sprintf("| %-32s | %-16s |\n", "Net.KeepAlive", "30s"))
	tableBuilder.WriteString(fmt.Sprintf("| %-32s | %-16s |\n", "Metadata.RefreshFrequency", "30m"))
	tableBuilder.WriteString(fmt.Sprintf("| %-32s | %-16t |\n", "Metadata.Full", false))
	tableBuilder.WriteString("+----------------------------------+------------------+\n")
	tableBuilder.WriteString(fmt.Sprintf("| %-32s | %-16s |\n", "CSV Log File", csvFilename))
	tableBuilder.WriteString(fmt.Sprintf("| %-32s | %-16s |\n", "Summary File", txtFilename))
	tableBuilder.WriteString("+----------------------------------+------------------+\n")

	tableOutput := tableBuilder.String()

	// Print to console
	fmt.Print(tableOutput)

	// Write to txt file
	err := os.WriteFile(txtFilename, []byte(tableOutput), 0644)
	if err != nil {
		log.Printf("Failed to write summary file: %v", err)
	} else {
		fmt.Printf("\nSummary saved to: %s\n", txtFilename)
	}
}

// percentile calculates the nth percentile of a sorted slice
func percentile(sorted []float64, n int) float64 {
	if len(sorted) == 0 {
		return 0
	}
	index := (n * len(sorted)) / 100
	if index >= len(sorted) {
		index = len(sorted) - 1
	}
	return sorted[index]
}
