package client

import (
	"fmt"
	"io"
	"net/http"
	"net/url"

	"github.com/cayleygraph/quad"
	"github.com/cayleygraph/quad/pquads"
)

func New(addr string) *Client {
	return &Client{addr: addr, cli: http.DefaultClient}
}

// Client is a struct used for communicating with a Cayley server through HTTP
// Deprecated: Client exists for backwards compatability. New code should use
// the updated client github.com/cayleygraph/go-client
type Client struct {
	addr string
	cli  *http.Client
}

func (c *Client) SetHTTPClient(cli *http.Client) {
	c.cli = cli
}
func (c *Client) url(s string, q map[string]string) string {
	addr := c.addr + s
	if len(q) != 0 {
		p := make(url.Values, len(q))
		for k, v := range q {
			p.Set(k, v)
		}
		addr += "?" + p.Encode()
	}
	return addr
}

type errRequestFailed struct {
	Status     string
	StatusCode int
}

func (e errRequestFailed) Error() string {
	return fmt.Sprintf("request failed: %d %v", e.StatusCode, e.Status)
}
func (c *Client) QuadReader() (quad.ReadCloser, error) {
	resp, err := http.Get(c.url("/api/v2/read", map[string]string{
		"format": "pquads",
	}))
	if err != nil {
		return nil, err
	}
	if resp.StatusCode != http.StatusOK {
		resp.Body.Close()
		return nil, errRequestFailed{StatusCode: resp.StatusCode, Status: resp.Status}
	}
	r := pquads.NewReader(resp.Body, 10*1024*1024)
	r.SetCloser(resp.Body)
	return r, nil
}

type funcCloser struct {
	f      func() error
	closed bool
}

func (c funcCloser) Close() error {
	if c.closed {
		return nil
	}
	c.closed = true
	return c.f()
}
func (c *Client) QuadWriter() (quad.WriteCloser, error) {
	pr, pw := io.Pipe()
	req, err := http.NewRequest("POST", c.url("/api/v2/write", nil), pr)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", pquads.ContentType)
	errc := make(chan error, 1)
	go func() {
		defer func() {
			close(errc)
			pr.Close()
		}()
		resp, err := c.cli.Do(req)
		if resp != nil && resp.Body != nil {
			defer resp.Body.Close()
		}
		if err == nil && resp.StatusCode != http.StatusOK {
			err = errRequestFailed{StatusCode: resp.StatusCode, Status: resp.Status}
		}
		errc <- err
	}()
	qw := pquads.NewWriter(pw, &pquads.Options{
		Full:   false,
		Strict: false,
	})
	qw.SetCloser(funcCloser{f: func() error {
		pw.Close()
		return <-errc
	}})
	return qw, nil
}
