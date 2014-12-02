package main

import (
	"flag"
	"fmt"
	"github.com/nqn/mesos-go-zk/detector"
)

// TODO(nnielsen): Don't depend on this prefix, but explode string by '_'.
func main() {
	zookeeperUrl := flag.String("zk", "zk://127.0.0.1:2181/mesos", "Zookeeper string")

	flag.Parse()

	d := detector.New(*zookeeperUrl)

	for {
		d.Detect()
		leader := <-d.Detected

		fmt.Printf("New leader detected: %v\n", leader)

		<-d.Disconnected

		fmt.Println("Disconnected - trying")
	}
}
