package sql

import (
	"github.com/fsouza/go-dockerclient"
	"github.com/google/cayley/graph"
	"github.com/google/cayley/graph/path2/pathtest"
	"github.com/lib/pq"
	"testing"
	"time"
)

func runPostgres(t testing.TB) (string, func()) {
	cl, err := docker.NewClient("unix:///var/run/docker.sock")
	if err != nil {
		t.Skip(err)
	}
	cont, err := cl.CreateContainer(docker.CreateContainerOptions{
		Config: &docker.Config{
			OpenStdin: true, Tty: true,
			Image: `postgres:9`,
			Env: []string{
				`POSTGRES_PASSWORD=postgres`,
			},
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

	ip := info.NetworkSettings.IPAddress
	ok := false
	for i := 0; i < 5; i++ {
		conn, err := pq.Open(`postgres://postgres:postgres@` + ip + `/postgres?sslmode=disable`)
		if err == nil {
			conn.Close()
			ok = true
		} else {
			time.Sleep(time.Second * 2)
		}
	}
	if !ok {
		t.Fatal("Container port is still closed.")
	}
	return ip, closer
}

func makeStore(t testing.TB) (graph.QuadStore, func()) {
	ip, closer := runPostgres(t)
	addr := `postgres://postgres:postgres@` + ip + `/postgres?sslmode=disable`
	if err := createSQLTables(addr, nil); err != nil {
		t.Fatal("Failed to create Postgres database:", err)
	}
	qs, err := newQuadStore(addr, nil)
	if err != nil {
		t.Fatal("Failed to create Postgres database:", err)
	}
	return qs, closer
}

func TestMorphisms(t *testing.T) {
	pathtest.TestMorphisms(t, makeStore)
}
