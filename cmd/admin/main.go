package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"text/tabwriter"

	pb "github.com/githubVladimirT/dekvs/proto"

	"google.golang.org/grpc"
)

func main() {
	addr := flag.String("addr", "", "gRPC address of cluster node")
	command := flag.String("cmd", "status", "Command: status, remove-node, join-node")
	nodeID := flag.String("id", "", "Node ID for remove/join commands")
	nodeRaftAddr := flag.String("raft-addr", "", "Raft address for join command")

	flag.Parse()

	conn, err := grpc.Dial(*addr, grpc.WithInsecure())
	if err != nil {
		log.Fatal(err)
	}
	defer conn.Close()

	client := pb.NewDeKVSClient(conn)

	switch *command {
	case "status":
		printClusterStatus(client)
	case "remove-node":
		removeNode(client, *nodeID)
	case "join-node":
		addNode(client, *nodeID, *nodeRaftAddr)
	default:
		log.Fatalf("Unknown command: %s", *command)
	}
}

func addNode(client pb.DeKVSClient, nodeID, nodeAddr string) {
	if nodeID == "" || nodeAddr == "" {
		log.Fatal("Both node ID and address are required for add-node")
	}

	_, err := client.Join(context.Background(), &pb.JoinRequest{
		NodeId:  nodeID,
		Address: nodeAddr,
	})

	if err != nil {
		log.Fatal("Failed to add node:", err)
	}

	fmt.Printf("Node %s added successfully\n", nodeID)
}

func printClusterStatus(client pb.DeKVSClient) {
	resp, err := client.GetClusterState(context.Background(), &pb.ClusterStateRequest{})
	if err != nil {
		log.Fatal(err)
	}

	w := tabwriter.NewWriter(os.Stdout, 0, 0, 2, ' ', 0)
	fmt.Fprintf(w, "Leader:\t%s\n", resp.LeaderId)
	fmt.Fprintln(w, "\nNodes:")
	fmt.Fprintln(w, "ID\tAddress\tHealthy\tState")
	fmt.Fprintln(w, "--\t-------\t-------\t-----")

	for _, node := range resp.Nodes {
		healthy := "✓"
		if !node.Healthy {
			healthy = "✗"
		}
		fmt.Fprintf(w, "%s\t%s\t%s\t%s\n", node.Id, node.Address, healthy, node.State)
	}
	w.Flush()
}

func removeNode(client pb.DeKVSClient, nodeID string) {
	if nodeID == "" {
		log.Fatal("Node ID required")
	}

	resp, err := client.RemoveNode(context.Background(), &pb.RemoveNodeRequest{NodeId: nodeID})
	if err != nil {
		log.Fatal(err)
	}

	if resp.Success {
		fmt.Printf("Node %s removed successfully\n", nodeID)
	} else {
		fmt.Printf("Failed to remove node: %s\n", resp.Error)
	}
}
