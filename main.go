package main

import (
	"flag"
	"github.com/hyperledger/fabric/common/flogging"
	"os"
	"simple_kv/network"
	"simple_kv/raft"
	"strings"
	"time"
)

const logFormat = "%{color}%{time:2006-01-02 15:04:05.000 MST} [%{module}] %{shortfunc} -> %{level:.4s} %{id:03x}%{color:reset} %{message}"

var logger = flogging.MustGetLogger("kv")

var (
	id      int
	port    int
	cluster string
	join    bool
)

func init() {
	flogging.Init(flogging.Config{
		Format:  logFormat,
		Writer:  os.Stderr,
		LogSpec: "INFO",
	})

	flag.IntVar(&id, "id", 1, "node ID")
	flag.IntVar(&port, "port", 8081, "http server port")
	flag.StringVar(&cluster, "cluster", "127.0.0.1:9091", "comma separated cluster peers")
	flag.BoolVar(&join, "join", false, "join to already exist cluster")
}

func main() {

	flag.Parse()

	node := raft.NewNode()
	server := NewHttpServer(node)
	go func() {
		err := server.start(port)
		if err != nil {
			logger.Error(err)
		}
	}()

	cls := strings.Split(cluster, ",")


	for i, peer := range cls {
		if i == id-1 {
			continue
		}

		wsClient := network.NewWSClient(peer)
		time.Sleep(time.Second)
		go wsClient.Send([]byte("hello"))
	}

	logger.Info("---->")
	ws := network.NewWSServer(cls[id-1], func(bytes []byte) {

	})
	ws.Start()
}
