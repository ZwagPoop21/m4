package main

import (
	pb "chat/grpc"
	"math/rand"
	"sync"
)

[]

type node struct {
	id int32
	server nodeServer
	timestamp int32
	nodes map[int32]string
	mu    sync.RWMutex
}

type nodeServer struct {
	pb.UnimplementedProcessServiceServer
}

func newNode(id int32, nodes map[int32]string) *Node {

	return node {
		id: id,
		server: 
	}
}

func (n *node) Process(stream pb.ProcessService_ProcessServer) error {
	outgoing := make(chan *pb.Message)


	go func() {
		for {

		}
	}()

}


