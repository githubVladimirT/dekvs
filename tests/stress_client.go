package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	pb "github.com/VT0x00/dekvs/proto"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/keepalive"
)

var keepaliveClientArgs = keepalive.ClientParameters{
	Time:                10 * time.Second,
	Timeout:             time.Second,
	PermitWithoutStream: true,
}

var (
	duration    = flag.Int("duration", 60, "Test duration in seconds")
	clients     = flag.Int("clients", 100, "Number of concurrent clients")
	requestSize = flag.Int("size", 256, "Request size in bytes")
	batchSize   = flag.Int("batch", 10, "Batch size")
	host        = flag.String("host", "localhost", "gRPC host")
	port        = flag.String("port", "8080", "gRPC port")
	testType    = flag.String("test", "mixed", "Test type: put, get, batch-put, batch-get, delete, mixed")
	keyPrefix   = flag.String("prefix", "stress", "Key prefix")
)

var (
	successCount int64
	failCount    int64
	totalLatency int64
)

type Client struct {
	conn   *grpc.ClientConn
	client pb.KVServiceClient
}

func NewClient(host, port string) (*Client, error) {
	conn, err := grpc.Dial(
		fmt.Sprintf("%s:%s", host, port),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithDefaultCallOptions(grpc.WaitForReady(true)),
		grpc.WithIdleTimeout(30*time.Second),
		grpc.WithKeepaliveParams(keepaliveClientArgs),
	)
	if err != nil {
		return nil, err
	}
	return &Client{
		conn:   conn,
		client: pb.NewKVServiceClient(conn),
	}, nil
}

func (c *Client) Close() error {
	return c.conn.Close()
}

func generateValue(size int) []byte {
	value := make([]byte, size)
	for i := 0; i < size; i++ {
		value[i] = byte(rand.Intn(256))
	}
	return value
}

func runPutWorker(ctx context.Context, client *Client, id int, wg *sync.WaitGroup) {
	defer wg.Done()

	for {
		select {
		case <-ctx.Done():
			return
		default:
		}

		key := fmt.Sprintf("%s:put:client%d:key%d", *keyPrefix, id, rand.Intn(10000))
		value := generateValue(*requestSize)

		start := time.Now()
		_, err := client.client.Put(ctx, &pb.PutRequest{
			Key:   key,
			Value: value,
		})
		latency := time.Since(start).Milliseconds()

		if err == nil {
			atomic.AddInt64(&successCount, 1)
			atomic.AddInt64(&totalLatency, latency)
		} else {
			atomic.AddInt64(&failCount, 1)
		}
	}
}

func runGetWorker(ctx context.Context, client *Client, id int, wg *sync.WaitGroup) {
	defer wg.Done()

	for {
		select {
		case <-ctx.Done():
			return
		default:
		}

		key := fmt.Sprintf("%s:get:prepop:%d", *keyPrefix, rand.Intn(100))

		start := time.Now()
		resp, err := client.client.Get(ctx, &pb.GetRequest{
			Key: key,
		})
		latency := time.Since(start).Milliseconds()

		if err == nil && resp.Found {
			atomic.AddInt64(&successCount, 1)
			atomic.AddInt64(&totalLatency, latency)
		} else {
			atomic.AddInt64(&failCount, 1)
		}
	}
}

func runBatchPutWorker(ctx context.Context, client *Client, id int, wg *sync.WaitGroup) {
	defer wg.Done()

	pairs := make([]*pb.KeyValue, *batchSize)
	for i := range pairs {
		pairs[i] = &pb.KeyValue{}
	}

	for {
		select {
		case <-ctx.Done():
			return
		default:
		}

		for j := range pairs {
			pairs[j].Key = fmt.Sprintf("%s:batch:client%d:key%d:item%d", *keyPrefix, id, rand.Intn(10000), j)
			pairs[j].Value = generateValue(*requestSize)
		}

		start := time.Now()
		_, err := client.client.BatchPut(ctx, &pb.BatchPutRequest{
			Pairs: pairs,
		})
		latency := time.Since(start).Milliseconds()

		if err == nil {
			atomic.AddInt64(&successCount, 1)
			atomic.AddInt64(&totalLatency, latency)
		} else {
			atomic.AddInt64(&failCount, 1)
		}
	}
}

func runBatchGetWorker(ctx context.Context, client *Client, id int, wg *sync.WaitGroup) {
	defer wg.Done()

	keys := make([]string, *batchSize)
	for {
		select {
		case <-ctx.Done():
			return
		default:
		}

		batchIdx := rand.Intn(10)
		for j := range keys {
			keys[j] = fmt.Sprintf("%s:batchget:prepop:batch%d:item%d", *keyPrefix, batchIdx, j)
		}

		start := time.Now()
		resp, err := client.client.BatchGet(ctx, &pb.BatchGetRequest{
			Keys: keys,
		})
		latency := time.Since(start).Milliseconds()

		if err == nil && len(resp.Values) > 0 {
			atomic.AddInt64(&successCount, 1)
			atomic.AddInt64(&totalLatency, latency)
		} else {
			atomic.AddInt64(&failCount, 1)
		}
	}
}

func runDeleteWorker(ctx context.Context, client *Client, id int, wg *sync.WaitGroup) {
	defer wg.Done()

	for {
		select {
		case <-ctx.Done():
			return
		default:
		}

		key := fmt.Sprintf("%s:del:client%d:key%d", *keyPrefix, id, rand.Intn(10000))
		value := generateValue(*requestSize)

		// First create the key
		client.client.Put(ctx, &pb.PutRequest{
			Key:   key,
			Value: value,
		})

		// Now delete it
		start := time.Now()
		_, err := client.client.Delete(ctx, &pb.DeleteRequest{
			Key: key,
		})
		latency := time.Since(start).Milliseconds()

		if err == nil {
			atomic.AddInt64(&successCount, 1)
			atomic.AddInt64(&totalLatency, latency)
		} else {
			atomic.AddInt64(&failCount, 1)
		}
	}
}

func runMixedWorker(ctx context.Context, client *Client, id int, wg *sync.WaitGroup) {
	defer wg.Done()

	for {
		select {
		case <-ctx.Done():
			return
		default:
		}

		op := rand.Intn(100)
		key := fmt.Sprintf("%s:mixed:client%d:key%d", *keyPrefix, id, rand.Intn(10000))
		value := generateValue(*requestSize)
		var err error
		var start time.Time
		var latency int64

		switch {
		case op < 40:
			// 40% PUT
			start = time.Now()
			_, err = client.client.Put(ctx, &pb.PutRequest{
				Key:   key,
				Value: value,
			})
			latency = time.Since(start).Milliseconds()
		case op < 70:
			// 30% GET
			start = time.Now()
			_, err = client.client.Get(ctx, &pb.GetRequest{
				Key: fmt.Sprintf("%s:mixed:client%d:key%d", *keyPrefix, id, rand.Intn(100)),
			})
			latency = time.Since(start).Milliseconds()
		case op < 85:
			// 15% Batch PUT
			start = time.Now()
			_, err = client.client.BatchPut(ctx, &pb.BatchPutRequest{
				Pairs: []*pb.KeyValue{
					{Key: key, Value: value},
					{Key: key + ":2", Value: value},
				},
			})
			latency = time.Since(start).Milliseconds()
		default:
			// 15% DELETE
			start = time.Now()
			_, err = client.client.Delete(ctx, &pb.DeleteRequest{
				Key: key,
			})
			latency = time.Since(start).Milliseconds()
		}

		if err == nil {
			atomic.AddInt64(&successCount, 1)
			atomic.AddInt64(&totalLatency, latency)
		} else {
			atomic.AddInt64(&failCount, 1)
		}
	}
}

func prepopulateKeys(host, port string, count int) error {
	log.Printf("Pre-populating %d keys...", count)

	client, err := NewClient(host, port)
	if err != nil {
		return err
	}
	defer client.Close()

	ctx := context.Background()
	for i := 0; i < count; i++ {
		key := fmt.Sprintf("%s:get:prepop:%d", *keyPrefix, i)
		value := generateValue(*requestSize)

		_, err := client.client.Put(ctx, &pb.PutRequest{
			Key:   key,
			Value: value,
		})
		if err != nil {
			log.Printf("Warning: failed to prepopulate key %s: %v", key, err)
		}
	}

	// Prepopulate batch keys
	for batch := 0; batch < 10; batch++ {
		for j := 0; j < *batchSize; j++ {
			key := fmt.Sprintf("%s:batchget:prepop:batch%d:item%d", *keyPrefix, batch, j)
			value := generateValue(*requestSize)

			_, err := client.client.Put(ctx, &pb.PutRequest{
				Key:   key,
				Value: value,
			})
			if err != nil {
				log.Printf("Warning: failed to prepopulate batch key %s: %v", key, err)
			}
		}
	}

	log.Printf("Keys pre-populated!")
	return nil
}

func printStatistics(testName string, duration time.Duration) {
	success := atomic.LoadInt64(&successCount)
	fail := atomic.LoadInt64(&failCount)
	total := success + fail
	latency := atomic.LoadInt64(&totalLatency)

	secs := duration.Seconds()
	rps := float64(success) / secs
	avgLatency := float64(0)
	if success > 0 {
		avgLatency = float64(latency) / float64(success)
	}
	successRate := float64(0)
	if total > 0 {
		successRate = float64(success) / float64(total) * 100
	}

	fmt.Printf("\n========================================\n")
	fmt.Printf("Stress Test Results: %s\n", testName)
	fmt.Printf("========================================\n")
	fmt.Printf("Duration:           %.1fs\n", secs)
	fmt.Printf("Total Requests:     %d\n", total)
	fmt.Printf("Successful:         %d\n", success)
	fmt.Printf("Failed:             %d\n", fail)
	fmt.Printf("Success Rate:       %.2f%%\n", successRate)
	fmt.Printf("Requests/sec:       %.0f\n", rps)
	fmt.Printf("Avg Latency:        %.2fms\n", avgLatency)
	fmt.Printf("========================================\n\n")

	// Production readiness check
	fmt.Printf("Production Readiness Assessment:\n")
	if successRate >= 99.0 && rps >= 1000 {
		fmt.Printf("✓ PASSED: System meets production requirements (>=1000 RPS, >=99%% success rate)\n")
	} else if successRate >= 95.0 && rps >= 500 {
		fmt.Printf("⚠ ACCEPTABLE: System is close to production requirements\n")
	} else {
		fmt.Printf("✗ FAILED: System does NOT meet production requirements\n")
		fmt.Printf("  Required: >=1000 RPS with >=99%% success rate\n")
		fmt.Printf("  Actual:   %.0f RPS with %.2f%% success rate\n", rps, successRate)
	}
}

func main() {
	flag.Parse()

	rand.Seed(time.Now().UnixNano())

	// Prepopulate keys for GET tests
	if *testType == "get" || *testType == "batch-get" || *testType == "mixed" {
		if err := prepopulateKeys(*host, *port, 100); err != nil {
			log.Printf("Warning: prepopulation failed: %v", err)
		}
	}

	// Create connection pool (one connection per 10 clients)
	numConnections := (*clients + 9) / 10
	clients_pool := make([]*Client, numConnections)
	for i := range clients_pool {
		client, err := NewClient(*host, *port)
		if err != nil {
			log.Fatalf("Failed to create client %d: %v", i, err)
		}
		clients_pool[i] = client
	}

	ctx, cancel := context.WithTimeout(context.Background(), time.Duration(*duration)*time.Second)
	defer cancel()

	var wg sync.WaitGroup

	log.Printf("Starting stress test: %s (duration: %ds, clients: %d, size: %d bytes)",
		*testType, *duration, *clients, *requestSize)

	startTime := time.Now()

	for i := 0; i < *clients; i++ {
		client := clients_pool[i%len(clients_pool)]
		wg.Add(1)

		switch *testType {
		case "put":
			go runPutWorker(ctx, client, i, &wg)
		case "get":
			go runGetWorker(ctx, client, i, &wg)
		case "batch-put":
			go runBatchPutWorker(ctx, client, i, &wg)
		case "batch-get":
			go runBatchGetWorker(ctx, client, i, &wg)
		case "delete":
			go runDeleteWorker(ctx, client, i, &wg)
		case "mixed":
			go runMixedWorker(ctx, client, i, &wg)
		default:
			go runMixedWorker(ctx, client, i, &wg)
		}
	}

	wg.Wait()
	duration_actual := time.Since(startTime)

	printStatistics(*testType, duration_actual)

	// Cleanup
	for _, client := range clients_pool {
		client.Close()
	}
}
