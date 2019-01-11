package raft

import "sync"

type Node struct {
	isLeader bool
	kv       map[string]string
	mu       sync.RWMutex
}

func NewNode() *Node {
	n := Node{
		isLeader: false,
		kv:       map[string]string{},
	}
	return &n
}

func (n *Node) Set(key, value string) {
	n.mu.Lock()
	n.kv[key] = value
	n.mu.Unlock()
}

func (n *Node) Get(key string) string {
	n.mu.RLock()
	value, ok := n.kv[key]
	n.mu.RUnlock()
	if !ok {
		return ""
	}
	return value
}

func (n *Node) Delete(key string) {
	n.mu.Lock()
	delete(n.kv, key)
	n.mu.Unlock()
}
