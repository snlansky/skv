package network

import (
	"github.com/gorilla/websocket"
	"github.com/hyperledger/fabric/common/flogging"
	"net/http"
	"sync"
	"time"
)

var logger = flogging.MustGetLogger("ws")

const (
	// Time allowed to write a message to the peer.
	writeWait = 10 * time.Second

	// Time allowed to read the next pong message from the peer.
	pongWait = 10 * time.Minute

	// Send pings to peer with this period. Must be less than pongWait.
	pingPeriod = (pongWait * 9) / 10

	// Maximum message size allowed from peer.
	maxMessageSize = 512
)

type WSServer struct {
	addr    string
	upper   *websocket.Upgrader
	clients *sync.Map
	handler func([]byte)
}

func NewWSServer(addr string, handler func([]byte)) *WSServer {
	upper := &websocket.Upgrader{
		ReadBufferSize:  1024,
		WriteBufferSize: 1024,
		CheckOrigin:     func(_ *http.Request) bool { return true },
	}

	return &WSServer{
		addr:    addr,
		upper:   upper,
		clients: &sync.Map{},
		handler: handler,
	}
}

func (ws *WSServer) Start() {
	http.HandleFunc("/ws", func(w http.ResponseWriter, r *http.Request) {
		conn, err := ws.upper.Upgrade(w, r, nil)
		if err != nil {
			logger.Errorf("upgrade: %v", err)
			return
		}
		ws.connHandler(conn)
	})
	err := http.ListenAndServe(ws.addr, nil)
	if err != nil {
		logger.Fatal("ListenAndServe: %v", err)
	}
}

func (ws *WSServer) connHandler(conn *websocket.Conn) {
	conn.SetReadLimit(maxMessageSize)
	err := conn.SetReadDeadline(time.Now().Add(pongWait))
	if err != nil {
		logger.Error(err)
		return
	}
	conn.SetPongHandler(func(string) error { conn.SetReadDeadline(time.Now().Add(pongWait)); return nil })

	logger.Infof("new connection at: %v", conn.RemoteAddr())
	client := NewClient(conn)

	ws.clients.Store(client, struct{}{})
	client.Start()
	ws.clients.Load(client)
	logger.Infof("connection stop: %v", conn.RemoteAddr())
}

func (ws *WSServer) Broadcast(data []byte) {
	ws.clients.Range(func(key, value interface{}) bool {
		client := key.(*Client)
		client.Send(data)
		return true
	})
}
