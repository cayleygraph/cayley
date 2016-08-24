package etcd

import (
	"fmt"
	"github.com/cayleygraph/cayley/internal/dock"
	"github.com/coreos/etcd/clientv3"
	"github.com/coreos/etcd/etcdserver/api/v3rpc/rpctypes"
	"golang.org/x/net/context"
	"google.golang.org/grpc/codes"
	"testing"
	"time"
)

const version = "v3.0.6"

func runEtcd3(t testing.TB) (*clientv3.Client, func()) {
	var conf dock.Config

	conf.Image = "quay.io/coreos/etcd:" + version
	conf.Cmd = []string{
		`etcd`,
		`--listen-client-urls=http://0.0.0.0:2379`,
		`--advertise-client-urls=http://localhost:2379`,
	}

	addr, closer := dock.Run(t, conf)
	etc, err := clientv3.New(clientv3.Config{
		Endpoints:   []string{"http://" + addr + ":2379"},
		DialTimeout: 20 * time.Second,
	})
	if err != nil {
		closer()
		t.Fatal(err)
	}
	etcCloser := func() {
		etc.Close()
		closer()
	}
	ctx, cancel := context.WithTimeout(context.TODO(), time.Minute)
	defer cancel()
	for {
		_, err := etc.MemberList(ctx)
		if err == nil {
			break
		}
		if e, ok := err.(rpctypes.EtcdError); ok {
			if e.Code() == codes.Unavailable {
				select {
				case <-ctx.Done():
					err = ctx.Err()
				case <-time.After(time.Second):
					continue
				}
			}
		}
		if err != nil {
			etcCloser()
			t.Fatal(err)
		}
	}
	return etc, etcCloser
}

func TestIterator(t *testing.T) {
	etc, closer := runEtcd3(t)
	defer closer()
	ctx := context.TODO()
	const cnt = 50
	if _, err := etc.Put(ctx, "m", ""); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < cnt; i++ {
		if _, err := etc.Put(ctx, fmt.Sprintf("n%04x", i), ""); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := etc.Put(ctx, "o", ""); err != nil {
		t.Fatal(err)
	}
	it := NewIterator(etc, "n", 0, clientv3.WithLimit(10))
	n := 0
	for ; it.Next(); n++ {
		if string(it.Result().Key) != fmt.Sprintf("n%04x", n) {
			t.Fatal("unexpected key:", string(it.Result().Key))
		}
	}
	if err := it.Err(); err != nil {
		t.Fatal(err)
	} else if n != cnt {
		t.Fatal("unexpected nodes count:", n)
	} else if it.requests != cnt/10 {
		t.Fatal("unexpected requests count:", it.requests)
	}
}
