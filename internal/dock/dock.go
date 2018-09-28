// +build docker

package dock

import (
	"fmt"
	"math/rand"
	"net"
	"runtime"
	"strconv"
	"testing"
	"time"

	"github.com/fsouza/go-dockerclient"
)

var (
	Address = `unix:///var/run/docker.sock`
)

type Config struct {
	docker.Config
}

type fullConfig struct {
	docker.Config
	docker.HostConfig
}

func run(t testing.TB, conf fullConfig) (addr string, closer func()) {
	if testing.Short() {
		t.SkipNow()
	}
	cli, err := docker.NewClient(Address)
	if err != nil {
		t.Fatal(err)
	}

	// If there is not relevant image at local, pull image from remote repository.
	if err := cli.PullImage(
		docker.PullImageOptions{
			Repository: conf.Image,
		},
		docker.AuthConfiguration{},
	); err != nil {
		// If pull image fail, skip the test.
		t.Skip(err)
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

func randPort() int {
	const (
		min = 10000
		max = 30000
	)
	for {
		port := min + rand.Intn(max-min)
		c, err := net.DialTimeout("tcp", fmt.Sprintf("%s:%d", localhost, port), time.Second)
		if c != nil {
			c.Close()
		}
		if err != nil {
			// TODO: check for a specific error
			return port
		}
	}
}

const localhost = "127.0.0.1"

func RunAndWait(t testing.TB, conf Config, port string, check func(string) bool) (addr string, closer func()) {
	fconf := fullConfig{Config: conf.Config}
	if runtime.GOOS != "linux" {
		lport := strconv.Itoa(randPort())
		// nothing except Linux runs Docker natively,
		// so we randomize the port and expose it on Docker VM
		fconf.PortBindings = map[docker.Port][]docker.PortBinding{
			docker.Port(port + "/tcp"): {{
				HostIP:   localhost,
				HostPort: lport,
			}},
		}
		port = lport
	}
	addr, closer = run(t, fconf)
	if runtime.GOOS != "linux" {
		// VM ports are automatically exposed on localhost
		addr = localhost
	}
	addr += ":" + port
	if check == nil {
		check = waitPort
	}
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

const wait = time.Second * 5

func waitPort(addr string) bool {
	start := time.Now()
	c, err := net.DialTimeout("tcp", addr, wait)
	if err == nil {
		c.Close()
	} else if dt := time.Since(start); dt < wait {
		time.Sleep(wait - dt)
	}
	return err == nil
}
