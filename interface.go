package main

type KV interface {
	Set(key, value string)
	Get(key string) string
	Delete(key string)
}

type Transport interface {
	Broadcast([]byte)
}
