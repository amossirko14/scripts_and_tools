package main

import (
	"hash/fnv"
	"fmt"
	"os"
)

// default offset32 = 2166136261
func HashValue(key string) uint32 {
	h := fnv.New32a()
	h.Write([]byte(key))
	return h.Sum32()
}

func main() {
	keys := os.Args[1:]
	for _, v := range keys {
		fmt.Println(HashValue(v))
	}
}
