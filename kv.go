package main

import (
	"encoding/json"
	"skv/raft"
	"sync"
)

type KV interface {
	Set(key, value string) error
	Get(key string) string
}

type kvpair struct {
	Key   string
	Value string
}

type MemKV struct {
	m        map[string]string
	mu       sync.Mutex
	proposal raft.Proposal
}

func NewMemKV(proposal raft.Proposal, commit <-chan raft.ApplyMsg) *MemKV {
	mk := MemKV{
		m:        map[string]string{},
		proposal: proposal,
	}
	go func() {
		for msg := range commit {
			var pair kvpair
			err := json.Unmarshal(msg.Command, &pair)
			if err != nil {
				panic(err)
			}
			mk.set(pair.Key, pair.Value)
		}
	}()

	return &mk
}

func (kv *MemKV) Set(key, value string) error {
	pair := &kvpair{Key: key, Value: value}
	buf, err := json.Marshal(pair)
	if err != nil {
		return err
	}
	return kv.proposal.ProposalCommand(buf)
}

func (kv *MemKV) set(key, value string) {
	kv.mu.Lock()
	kv.m[key] = value
	kv.mu.Unlock()
}

func (kv *MemKV) Get(key string) string {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	return kv.m[key]
}
