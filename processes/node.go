package main

import (
	pb "chat/grpc"
	"context"
	"flag"
	"fmt"
	"log"
	"net"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type node struct {
	pb.UnimplementedProcessServiceServer
	id               int32
	port             string
	state            string //RELEASED, HELD, WANTED
	timestamp        int32
	mu               sync.RWMutex
	nodes            map[int32]string //ID -> port
	streams          map[int32]pb.ProcessService_ProcessClient
	repliesRecieved  int     //When we request to enter CS we need this number to be == amount of other nodes because then all others have agreed to let US enter.
	requestTimestamp int32   //We have to save the exact moment we request access to CS, we cant just use timestamp as this updates when we recieve replies and send requests
	standbyNodes     []int32 //This is a waitinglist that the current best candidate holds of the other nodes used to later when releasing CS access to let other nodes know.
}

// When other nodes connects to this nodes server
func (n *node) Process(stream pb.ProcessService_ProcessServer) error {
	for {
		msg, _ := stream.Recv()
		n.mu.Lock()
		n.timestamp = max(n.timestamp, msg.Timestamp) + 1
		n.mu.Unlock()

		if msg.State == "WANTED" {
			n.handleWantedRequest(msg)
		} else if msg.State == "RELEASED" {
			n.handleReleasedReply(msg)
		}
	}
}

func (n *node) handleWantedRequest(msg *pb.Message) {
	log.Printf("Retrieved at {local timestamp: %s} wanted request from node: {id: %d, port: %s, timestamp: %s} \n", n.timestamp, msg.Id, n.nodes[msg.Id], msg.Timestamp)
	n.mu.Lock()
	defer n.mu.Unlock()

	if n.state == "HELD" || (n.state == "WANTED" &&
		((n.requestTimestamp < msg.Timestamp) || ((n.requestTimestamp == msg.Timestamp) && n.id < msg.Id))) {
		n.standbyNodes = append(n.standbyNodes, msg.Id)
	} else {
		n.reply(msg.Id)
	}
}

func (n *node) handleReleasedReply(msg *pb.Message) {
	log.Printf("Retrieved at {local timestamp: %s} released reply from node: {id: %d, port: %s, timestamp: %s} \n", n.timestamp, msg.Id, n.nodes[msg.Id], msg.Timestamp)
	n.mu.Lock()
	if n.state == "WANTED" {
		n.repliesRecieved++

		if n.repliesRecieved == (len(n.nodes) - 1) {
			n.state = "HELD"
			n.mu.Unlock()
			n.enterCS()
		} else {
			n.mu.Unlock()
		}
		return
	}
	n.mu.Unlock()
}

func (n *node) reply(id int32) {
	log.Printf("Replying (RELEASED) at {local timestamp: %s} to node: {id: %d, port: %s} \n", n.timestamp, id, n.nodes[id])
	n.timestamp++
	msg := &pb.Message{
		Id:        n.id,
		Timestamp: n.timestamp,
		State:     "RELEASED",
	}
	n.streams[id].Send(msg)
}

func (n *node) enterCS() {
	log.Printf("Entered CRITICAL SECTION at {local timestamp: %s} \n", n.timestamp)
	time.Sleep(3 * time.Second)

	n.mu.Lock()
	n.state = "RELEASED"
	n.mu.Unlock()
	log.Printf("Left CRITICAL SECTION at {local timestamp: %s} \n", n.timestamp)

	for _, id := range n.standbyNodes {
		n.reply(id)
	}
}

func (n *node) requestCS() {
	n.mu.Lock()
	n.state = "WANTED"
	n.timestamp++
	n.requestTimestamp = n.timestamp
	n.repliesRecieved = 0
	log.Printf("Requesting at {local timestamp: %s} to enter CRITICAL SECTION \n", n.timestamp)
	n.mu.Unlock()

	for id, stream := range n.streams {
		n.timestamp++
		request := &pb.Message{
			Id:        n.id,
			Timestamp: n.timestamp,
			State:     n.state,
		}
		stream.Send(request)
		log.Printf("Requested at {local timestamp: %s} access to CRITICAL SECTION from node: {id: %d, port: %s} \n", n.timestamp, id, n.nodes[id])
	}
}

func main() {
	id := flag.Int("id", 0, "Choose an id")
	flag.Parse()

	otherProcesses := map[int32]string{
		1: ":5001",
		2: ":5002",
		3: ":5003",
	}

	port := otherProcesses[int32(*id)]
	delete(otherProcesses, int32(*id))

	n := &node{
		id:               int32(*id),
		port:             port,
		state:            "RELEASED",
		timestamp:        0,
		mu:               sync.RWMutex{},
		nodes:            otherProcesses,
		streams:          make(map[int32]pb.ProcessService_ProcessClient),
		repliesRecieved:  0,
		requestTimestamp: 0,
		standbyNodes:     []int32{},
	}

	tpcListener, err := net.Listen("tcp", n.port)
	if err != nil {
		log.Printf("ERROR creating the TCP listener:", err)
		log.Fatal(err)
		return
	}
	nodeServer := grpc.NewServer()
	pb.RegisterProcessServiceServer(nodeServer, n)

	log.Printf("Server started on node {id: %d, port: %s} \n", n.id, n.port)

	go func() {
		for {
			if len(n.streams) >= 2 {
				break
			}
			for id, port := range n.nodes {
				if _, ok := n.streams[id]; ok {
					continue
				}
				conn, err := grpc.NewClient(
					fmt.Sprintf("localhost%s", port),
					grpc.WithTransportCredentials(insecure.NewCredentials()),
				)
				if err != nil {
					continue
				}

				stream, err := pb.NewProcessServiceClient(conn).Process(context.Background())
				if err != nil {
					conn.Close()
					continue
				}
				n.streams[id] = stream
			}
			time.Sleep(5 * time.Second)
		}
	}()

	go func() {
		if err := nodeServer.Serve(tpcListener); err != nil {
			log.Println("ERROR SERVING", err)
		}
	}()

	shouldClose := false
	//Simulations loop
	for {
		if shouldClose {
			break
		}

		fmt.Scanln()
		if n.state == "RELEASED" {
			n.requestCS()
		}
	}

	stop := make(chan os.Signal, 1)
	signal.Notify(stop, os.Interrupt, syscall.SIGTERM)
	defer signal.Stop(stop)
	go func() {
		<-stop
		nodeServer.GracefulStop()
		shouldClose = true
	}()

}
