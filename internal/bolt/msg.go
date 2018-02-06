package bolt

func init() {
	RegisterMsg(
		InitMsg{},
		SuccessMsg{},
		FailureMsg{},
		IgnoredMsg{},
		RunMsg{},
		PullAllMsg{},
		DiscardAllMsg{},
		AckFailureMsg{},
		ResetMsg{},
		Record{},
		Node{},
		Relationship{},
	)
}

var signatures = make(map[byte]Msg)

func RegisterMsg(arr ...Msg) {
	for _, m := range arr {
		signatures[m.Signature()] = m
	}
}

type Msg interface {
	Signature() byte
}

type InitMsg struct {
	ClientName string
	AuthToken  map[string]interface{}
}

func (InitMsg) Signature() byte {
	return 0x01
}

type RunMsg struct {
	Statement  string
	Parameters map[string]interface{}
}

func (RunMsg) Signature() byte {
	return 0x10
}

type SuccessMsg struct {
	Metadata map[string]interface{}
}

func (SuccessMsg) Signature() byte {
	return 0x70
}

type FailureMsg struct {
	Metadata map[string]interface{}
}

func (FailureMsg) Signature() byte {
	return 0x7F
}

type IgnoredMsg struct {
	Metadata map[string]interface{}
}

func (IgnoredMsg) Signature() byte {
	return 0x7E
}

type PullAllMsg struct{}

func (PullAllMsg) Signature() byte {
	return 0x3F
}

type DiscardAllMsg struct{}

func (DiscardAllMsg) Signature() byte {
	return 0x2F
}

type AckFailureMsg struct{}

func (AckFailureMsg) Signature() byte {
	return 0x0E
}

type ResetMsg struct{}

func (ResetMsg) Signature() byte {
	return 0x0F
}

type Record struct {
	Fields []interface{}
}

func (Record) Signature() byte {
	return 0x71
}

type Node struct {
	ID     int64
	Labels []string
	Fields map[string]interface{}
}

func (Node) Signature() byte {
	return 0x4E
}

type Relationship struct {
	ID       int64
	From, To int64
	Type     string
	Fields   map[string]interface{}
}

func (Relationship) Signature() byte {
	return 0x52
}
