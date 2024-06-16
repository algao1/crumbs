package node

import (
	"context"
	"crumbs/p2p-rpc/service"
	"fmt"
	"log"
	"net"
	"time"

	"github.com/hashicorp/consul/api"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

const NodePrefix = "NODE_"

type Node struct {
	Name       string
	Address    string
	Peers      map[string]service.PingServiceClient
	ServDiscKV *api.KV

	service.UnimplementedPingServiceServer
}

func New(name, address, sdAddress string) *Node {
	config := api.DefaultConfig()
	consul, err := api.NewClient(config)
	if err != nil {
		log.Fatalf("unable to contact service discovery: %v", err)
	}

	n := &Node{
		Name:       name,
		Address:    address,
		Peers:      make(map[string]service.PingServiceClient),
		ServDiscKV: consul.KV(),
	}
	err = n.register()
	if err != nil {
		log.Fatal(err)
	}

	go n.listen()
	go n.fetchPeers()
	return n
}

func (n *Node) Ping(ctx context.Context, req *service.PingRequest) (*service.PongReply, error) {
	return &service.PongReply{Message: "pong!"}, nil
}

func (n *Node) PingAll(ctx context.Context) {
	for peer, client := range n.Peers {
		resp, err := client.Ping(ctx, &service.PingRequest{Name: peer})
		if err != nil {
			log.Printf("[%s] failed to get response from %s\n", n.Name, peer)
			continue
		}
		log.Printf("[%s] got response from %s: %s\n", n.Name, peer, resp.GetMessage())
	}
}

func (n *Node) register() error {
	p := &api.KVPair{Key: NodePrefix + n.Name, Value: []byte(n.Address)}
	_, err := n.ServDiscKV.Put(p, nil)
	if err != nil {
		return fmt.Errorf("unable to register with service discovery: %w", err)
	}
	return nil
}

func (n *Node) listen() {
	lis, err := net.Listen("tcp", n.Address)
	if err != nil {
		log.Fatalf("failed to listen: %v", err)
	}

	serv := grpc.NewServer()
	service.RegisterPingServiceServer(serv, n)

	if err := serv.Serve(lis); err != nil {
		log.Fatalf("failed to serve: %v", err)
	}
}

func (n *Node) fetchPeers() {
	ticker := time.NewTicker(5 * time.Second)
	for {
		<-ticker.C

		kvPairs, _, err := n.ServDiscKV.List(NodePrefix, nil)
		if err != nil {
			log.Printf("[%s] unable to fetch kv pairs: %v\n", n.Name, err)
			continue
		}

		for _, kv := range kvPairs {
			if kv.Key == NodePrefix+n.Name {
				continue
			}
			if _, ok := n.Peers[kv.Key]; ok {
				continue
			}

			client, err := createClient(string(kv.Value))
			if err != nil {
				log.Printf("[%s] unable to create client for %s\n", n.Name, kv.Key)
				continue
			}
			n.Peers[kv.Key] = client
			log.Printf("[%s] added new member: %s\n", n.Name, kv.Key)
		}
	}
}

func createClient(addr string) (service.PingServiceClient, error) {
	conn, err := grpc.Dial(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, fmt.Errorf("did not connect: %w", err)
	}
	return service.NewPingServiceClient(conn), nil
}
