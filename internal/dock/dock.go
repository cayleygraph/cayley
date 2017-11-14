// +build docker

package dock

import (
	"net"
	"testing"
	"time"

	"github.com/fsouza/go-dockerclient"
)

var (
	Address = `unix:///var/run/docker.sock`
)

type Config struct {
	docker.Config
	docker.HostConfig
}

func Run(t testing.TB, conf Config) (addr string, closer func()) {
	if testing.Short() {
		t.SkipNow()
	}
	cli, err := docker.NewClient(Address)
	if err != nil {
		t.Fatal(err)
	}
	cont, err := cli.CreateContainer(docker.CreateContainerOptions{
		Config:     &conf.Config,
		HostConfig: &conf.HostConfig,
	})
	if err != nil {
		t.Skip(err)
	}

	closer = func() {
		cli.RemoveContainer(docker.RemoveContainerOptions{
			ID:    cont.ID,
			Force: true,
		})
	}

	if err := cli.StartContainer(cont.ID, &conf.HostConfig); err != nil {
		closer()
		t.Skip(err)
	}

	info, err := cli.InspectContainer(cont.ID)
	if err != nil {
		closer()
		t.Skip(err)
	}
	addr = info.NetworkSettings.IPAddress
	return
}

func RunAndWait(t testing.TB, conf Config, check func(string) bool) (addr string, closer func()) {
	addr, closer = Run(t, conf)
	ok := false
	for i := 0; i < 10 && !ok; i++ {
		ok = check(addr)
		if !ok {
			time.Sleep(time.Second * 2)
		}
	}
	if !ok {
		closer()
		t.Fatal("Container check fails.")
	}
	return addr, closer
}

func WaitPort(port string) func(addr string) bool {
	const wait = time.Second * 5
	return func(addr string) bool {
		start := time.Now()
		c, err := net.DialTimeout("tcp", addr+":"+port, wait)
		if err == nil {
			c.Close()
		} else if dt := time.Since(start); dt < wait {
			time.Sleep(wait - dt)
		}
		return err == nil
	}
}
