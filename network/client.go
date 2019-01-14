package network

import (
	"github.com/gorilla/websocket"
	"go.uber.org/atomic"
	"net/url"
	"time"
)

type Client struct {
	conn    *websocket.Conn
	queue   chan []byte
	isClose *atomic.Bool
}

func NewClient(conn *websocket.Conn) *Client {
	c := &Client{
		conn:    conn,
		queue:   make(chan []byte),
		isClose: atomic.NewBool(false),
	}
	return c
}

func (c *Client) Start() {
	go func() {
		for {
			if c.isClose.Load() == true {
				return
			}
			data, ok := <-c.queue
			if !ok {
				c.Close()
				return
			}
			err := c.conn.WriteMessage(websocket.TextMessage, data)
			if err != nil {
				logger.Errorf("send %s message error:%s", c.conn.RemoteAddr(), err)
				c.Close()
			}
		}
	}()

	for {
		_, message, err := c.conn.ReadMessage()
		if err != nil {
			if websocket.IsUnexpectedCloseError(err, websocket.CloseGoingAway, websocket.CloseAbnormalClosure) {
				logger.Infof("connection closed: %s, %v", c.conn.RemoteAddr(), err)
			} else {
				logger.Infof("read message error: %s, %v", c.conn.RemoteAddr(), err)

			}
			c.Close()
			return
		}
		logger.Infof("get message %s", string(message))
		//c.Send(message)
	}
}

func (c *Client) Send(data []byte) {
	if c.isClose.Load() == true {
		return
	}
	c.queue <- data
}

func (c *Client) Close() {
	if c.isClose.CAS(false, true) {
		close(c.queue)
		err := c.conn.Close()
		if err != nil {
			logger.Errorf("close connection %v error:%v", c.conn.RemoteAddr(), err)
		}
	}
}

func NewWSConnection(addr string) (*websocket.Conn, error) {
	u := url.URL{Scheme: "ws", Host: addr, Path: "/ws"}
	conn, _, err := websocket.DefaultDialer.Dial(u.String(), nil)
	return conn, err
}

func Transport(addr string) *Client {
RECONNECT:
	conn, err := NewWSConnection(addr)
	if err != nil {
		logger.Errorf("connect [%s] error: %s", addr, err)
		time.Sleep(time.Second)
		goto RECONNECT
	}

	client := NewClient(conn)
	go client.Start()
	return client
}
