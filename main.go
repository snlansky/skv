package main

import (
	"flag"
	"github.com/snlansky/glibs/logging"
	"net"
	"net/rpc"
	"skv/raft"
	"strings"
)

var logger = logging.MustGetLogger("kv")

var (
	id      int
	port    int
	cluster string
	join    bool
)

func init() {
	flag.IntVar(&id, "id", 1, "node ID")
	flag.IntVar(&port, "port", 8081, "http server port")
	flag.StringVar(&cluster, "cluster", "127.0.0.1:9091", "comma separated cluster peers")
	flag.BoolVar(&join, "join", false, "join to already exist cluster")
}

type client struct {
	addr   string
	client *rpc.Client
}

func newClient(addr string) *client {
	return &client{addr: addr}
}

func (c *client) Call(serviceMethod string, args interface{}, reply interface{}) error {
	if c.client == nil {
		client, err := rpc.Dial("tcp", c.addr)
		if err != nil {
			//logger.Errorf("connect [%s] error: %s", c.addr, err)
			return err
		}
		c.client = client
	}
	err := c.client.Call(serviceMethod, args, reply)
	if err != nil {
		c.client = nil
	}
	return err
}

func main() {

	flag.Parse()

	id = id - 1

	//node := raft.NewNode()
	//server := NewHttpServer(node)
	//go func() {
	//	err := server.start(port)
	//	if err != nil {
	//		logger.Error(err)
	//	}
	//}()

	addrs := strings.Split(cluster, ",")

	ln, err := net.Listen("tcp", addrs[id])
	if err != nil {
		logger.Fatalf("listen [%s] error:%s", addrs[id], err)
	}

	peer := make([]raft.RPC, len(addrs))

	for i, addr := range addrs {
		if i == id {
			continue
		}
		go func(id int, addr string) {
			peer[id] = newClient(addr)
		}(i, addr)
	}

	r := raft.NewRaft(id, peer)
	rpcServer := rpc.NewServer()
	err = rpcServer.Register(r)
	if err != nil {
		logger.Fatalf("rps register error:%s", err)
	}

	for {
		conn, err := ln.Accept()
		if err == nil {
			logger.Infof("new connection [%s]", conn.RemoteAddr().String())
			go rpcServer.ServeConn(conn)
		} else {
			logger.Errorf("accept error:%s", err)
		}
	}
}
