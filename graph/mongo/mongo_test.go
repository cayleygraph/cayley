package mongo

import (
	"github.com/fsouza/go-dockerclient"
	"github.com/google/cayley/graph"
	"github.com/google/cayley/graph/path2/pathtest"
	"testing"
)

func runMongo(t testing.TB) (string, func()) {
	cl, err := docker.NewClient("unix:///var/run/docker.sock")
	if err != nil {
		t.Skip(err)
	}
	cont, err := cl.CreateContainer(docker.CreateContainerOptions{
		Config: &docker.Config{
			OpenStdin: true, Tty: true,
			Image: `mongo:3`,
		},
		HostConfig: &docker.HostConfig{},
	})
	if err != nil {
		t.Skip(err)
	}

	closer := func() {
		cl.RemoveContainer(docker.RemoveContainerOptions{ID: cont.ID, Force: true})
	}

	if err := cl.StartContainer(cont.ID, &docker.HostConfig{}); err != nil {
		closer()
		t.Skip(err)
	}

	info, err := cl.InspectContainer(cont.ID)
	if err != nil {
		closer()
		t.Skip(err)
	}
	return info.NetworkSettings.IPAddress, closer
}

func makeStore(t testing.TB) (graph.QuadStore, func()) {
	ip, closer := runMongo(t)
	qs, err := newQuadStore(ip+":27017", nil)
	if err != nil {
		t.Fatal("Failed to create MongoDB database.")
	}
	return qs, closer
}

func TestMorphisms(t *testing.T) {
	pathtest.TestMorphisms(t, makeStore)
}
