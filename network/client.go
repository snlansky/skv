package network

import (
	"github.com/gorilla/websocket"
	"go.uber.org/atomic"
	//"golang.org/x/net/websocket"
	"net/url"
	"time"
)

type WSClient struct {
	addr    string
	queue   chan *[]byte
	isClose *atomic.Bool
}

func NewWSClient(addr string) *WSClient {
	c :=  &WSClient{
		addr:    addr,
		queue:   make(chan *[]byte),
		isClose: atomic.NewBool(false),
	}
	go c.start()
	return c
}

func (c *WSClient) start() {
	u := url.URL{Scheme: "ws", Host: c.addr, Path: "/ws"}

RECONNECT:
	conn, _, err := websocket.DefaultDialer.Dial(u.String(), nil)
	//ws, err := websocket.Dial(u.String(), "ws", "ws://"+c.addr+"/")
	if err != nil {
		logger.Errorf("connect [%s] error: %s", c.addr, err)
		time.Sleep(time.Second)
		goto RECONNECT
	}

	c.isClose.Store(false)

	go func() {
		for {
			if c.isClose.Load() == true {
				return
			}

			data := <-c.queue

			err := conn.WriteMessage(websocket.TextMessage, *data)
			if err != nil {
				logger.Errorf("write to [%s] message error:%s", c.addr, err)
				c.tryClose(conn)
				return
			}
		}
	}()

	for {
		_, data , err := conn.ReadMessage()
		if err != nil {
			logger.Errorf("read to [%s] message error:%s", c.addr, err)
			c.tryClose(conn)
		}

		logger.Infof("msg -> :%s", string(data))

	}
}

func (c *WSClient) tryClose(conn *websocket.Conn) {
	if c.isClose.CAS(false, true) {
		if conn == nil {
			return
		}
		err := conn.Close()
		if err != nil {
			logger.Errorf("close connect [%s] error:%s", c.addr, err)
		}
	}
}

func (c *WSClient) Send(data []byte) {
	if c.isClose.Load() == false {
		c.queue <- &data
	}
}
