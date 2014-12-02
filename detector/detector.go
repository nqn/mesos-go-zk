package detector

import (
	"code.google.com/p/goprotobuf/proto"
	"encoding/binary"
	"github.com/golang/glog"
	"github.com/nqn/mesos-go-zk/mesos"
	"github.com/samuel/go-zookeeper/zk"
	"math"
	"net"
	"net/url"
	"path"
	"strconv"
	"strings"
	"time"
)

const label string = "info"

type Host struct {
	Ip   string
	Port uint32
}

type Detector struct {
	Detected       chan Host
	Disconnected   chan bool
	connection     *zk.Conn
	zookeeperHosts []string
	zookeeperPath  string
}

func New(zookeeperUrl string) *Detector {
	u, err := url.Parse(zookeeperUrl)
	if err != nil {
		glog.Fatal(err)
	}
	Path := u.Path
	Hosts := strings.Split(u.Host, ",")

	d := &Detector{
		Detected:       make(chan Host, 1),
		Disconnected:   make(chan bool, 1),
		zookeeperHosts: Hosts,
		zookeeperPath:  Path,
	}

	connection, _, err := zk.Connect(Hosts, time.Second)
	if err != nil {
		glog.Fatal(err)
	}

	d.connection = connection

	return d
}

// TODO(nnielsen): Parse zookeeper path.
func (d *Detector) Detect() {
	detect := func() bool {
		children, _, err := d.connection.Children(d.zookeeperPath)
		if err != nil {
			glog.Warning("Could not read children at path ", d.zookeeperPath)
			return false
		}

		// TODO(nnielsen): Install group watcher.
		var leaderSequence uint64 = math.MaxUint64
		var leaderPath = ""
		for _, node := range children {
			sequenceString := strings.TrimPrefix(node, label+"_")

			sequence, err := strconv.ParseUint(sequenceString, 10, 64)
			if err != nil {
				continue
			}

			if sequence < leaderSequence {
				leaderSequence = sequence
				leaderPath = path.Join(d.zookeeperPath, label+"_"+sequenceString)
			}
		}

		if leaderSequence == math.MaxUint64 {
			glog.V(2).Info("No masters found at path ", d.zookeeperPath)
			return false
		}

		glog.V(2).Infof("Leader sequence: %d", leaderSequence)
		glog.V(2).Infof("Trying to get sequence: %s", leaderPath)

		leaderData, _, leaderEvent, err := d.connection.GetW(leaderPath)
		if err != nil {
			panic(err)
		}

		master := new(mesos.MasterInfo)
		err = proto.Unmarshal(leaderData, master)
		if err != nil {
			panic(err)
		}

		ip := make(net.IP, 4)
		binary.LittleEndian.PutUint32(ip, *master.Ip)

		glog.V(2).Infof("Leader: %s (ip: %s port: %d)\n", *master.Hostname, ip.String(), *master.Port)

		go func() {
			// Watch for change (disconnects and new-comers).
			e := <-leaderEvent
			if e.Type == zk.EventNodeDeleted {
				if e.Path == leaderPath {
					d.Disconnected <- true
				}
			}
		}()

		d.Detected <- Host{ip.String(), *master.Port}
		return true
	}

	go func() {
		detected := detect()
		timeout := 1 * time.Second
		for detected != true {
			detected = detect()
			glog.V(2).Infof("Retrying in %v\n", timeout)
			time.Sleep(timeout)
		}
	}()

}
