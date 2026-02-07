// client/main.go
package main

import (
	"context"
	"fmt"
	"log"

	//"time"

	pb "github.com/githubVladimirT/dekvs/proto"
	"google.golang.org/grpc"
)

func main() {
	conn, err := grpc.Dial("localhost:13001", grpc.WithInsecure())
	if err != nil {
		log.Fatal(err)
	}
	defer conn.Close()

	client := pb.NewDeKVSClient(conn)

	stats, err := client.Stats(context.Background(), &pb.StatsRequest{})
	if err != nil {
		log.Fatal(err)
	}

	fmt.Println("Cluster stats:")
	for k, v := range stats.Stats {
		fmt.Printf("  %s: %s\n", k, v)
	}

	// resp, err := client.Set(context.Background(), &pb.SetRequest{
	// 	Key:        "mykey",
	// 	Value:      []byte("myvalue"),
	// 	TtlSeconds: 60, // 1 minute
	// })
	// if err != nil {
	// 	log.Fatal(err)
	// }
	// log.Printf("Set version: %d", resp.Version)

	// getResp, err := client.Get(context.Background(), &pb.GetRequest{
	// 	Key: "mykey",
	// })
	// if err != nil {
	// 	log.Fatal(err)
	// }
	// log.Printf("Got value: %s", string(getResp.Value))

	// batchResp, err := client.Batch(context.Background(), &pb.BatchRequest{
	// 	Operations: []*pb.BatchOperation{
	// 		{Operation: &pb.BatchOperation_Set{
	// 			Set: &pb.SetRequest{Key: "key1", Value: []byte("value1")},
	// 		}},
	// 		{Operation: &pb.BatchOperation_Set{
	// 			Set: &pb.SetRequest{Key: "key2", Value: []byte("value2"), TtlSeconds: 30},
	// 		}},
	// 		{Operation: &pb.BatchOperation_Delete{Delete: "oldkey"}},
	// 	},
	// })
	// if err != nil {
	// 	log.Fatal(err)
	// }
	// log.Printf("Batch versions: %v", batchResp.Versions)
}
