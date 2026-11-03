package main

import (
	pb "chat/grpc"
	"fmt"
	"sync"
	"time"
)

type node struct {
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
	fmt.Print("Node connected....")

	for {
		msg, _ := stream.Recv()

		n.mu.Lock()
		n.timestamp++

		if msg.State == "WANTED" {
			n.handleWantedRequest(msg)
		} else if msg.State == "RELEASED" {
			n.handleReleasedReply(msg)
		}
	}
}

func (n *node) handleWantedRequest(msg *pb.Message) {
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
	n.mu.Lock()
	defer n.mu.Unlock()

	if n.state == "WANTED" {
		n.repliesRecieved++

		if n.repliesRecieved == (len(n.nodes) - 1) {
			n.state = "HELD"
			n.enterCS()
		}
	}
}

func (n *node) reply(id int32) {
	n.timestamp++
	msg := &pb.Message{
		Id:        n.id,
		Timestamp: n.timestamp,
		State:     "RELEASED",
	}
	n.streams[id].Send(msg)
}

func (n *node) enterCS() {
	fmt.Print("Entered Critical Section")
	time.Sleep(3 * time.Second)

	n.mu.Lock()
	fmt.Print("LEAVING Critical Section")
	n.state = "RELEASED"
	n.mu.Unlock()

	for _, id := range n.standbyNodes {
		n.reply(id)
	}
}

func main() {

}
