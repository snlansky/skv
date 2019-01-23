package main

import (
	"fmt"
	"github.com/gin-gonic/gin"
)

type HttpServer struct {
	kv KV
}

func NewHttpServer(kv KV) *HttpServer {
	return &HttpServer{kv: kv}
}

func (s *HttpServer) start(port int) error {
	addr := fmt.Sprintf(":%d", port)
	r := gin.Default()
	r.GET("/get", s.get)
	r.GET("/set", s.set)
	return r.Run(addr)
}

func (s *HttpServer) get(c *gin.Context) {
	key := c.Query("key")
	if key == "" {
		c.String(200, "key is null")
		return
	}

	value := s.kv.Get(key)
	c.JSON(200, gin.H{
		"key":   key,
		"value": value,
	})
}

func (s *HttpServer) set(c *gin.Context) {
	key := c.Query("key")
	if key == "" {
		c.String(200, "key is null")
		return
	}

	value := c.Query("value")
	if value == "" {
		c.String(200, "value is null")
		return
	}

	err := s.kv.Set(key, value)
	if err != nil {
		c.String(200, err.Error())
	} else {
		c.JSON(200, gin.H{
			"key":   key,
			"value": value,
		})
	}

}
