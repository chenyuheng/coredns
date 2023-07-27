package dnstap

import (
	"context"
	"time"

	"github.com/coredns/coredns/plugin"
	"github.com/coredns/coredns/plugin/dnstap/msg"
	"github.com/coredns/coredns/plugin/pkg/replacer"
	"github.com/coredns/coredns/plugin/pkg/dnstest"
	"github.com/coredns/coredns/request"

	tap "github.com/dnstap/golang-dnstap"
	"github.com/miekg/dns"
)

// Dnstap is the dnstap handler.
type Dnstap struct {
	Next plugin.Handler
	io   tapper
	repl replacer.Replacer
	ctx  context.Context
	state request.Request
	rrw  *dnstest.Recorder

	// IncludeRawMessage will include the raw DNS message into the dnstap messages if true.
	IncludeRawMessage bool
	Identity          []byte
	Version           []byte
	ExtraFormat       string
}

// TapMessage sends the message m to the dnstap interface.
func (h Dnstap) TapMessage(m *tap.Message) {
	t := tap.Dnstap_MESSAGE
	extraMsg := h.repl.Replace(h.ctx, h.state, h.rrw, h.ExtraFormat)
	dt := &tap.Dnstap{
		Type: &t,
		Message: m,
		Identity: h.Identity,
		Version: h.Version,
		Extra: []byte(extraMsg),
	}
	h.io.Dnstap(dt)
}

func (h Dnstap) tapQuery(w dns.ResponseWriter, query *dns.Msg, queryTime time.Time) {
	q := new(tap.Message)
	msg.SetQueryTime(q, queryTime)
	msg.SetQueryAddress(q, w.RemoteAddr())

	if h.IncludeRawMessage {
		buf, _ := query.Pack()
		q.QueryMessage = buf
	}
	msg.SetType(q, tap.Message_CLIENT_QUERY)
	h.TapMessage(q)
}

// ServeDNS logs the client query and response to dnstap and passes the dnstap Context.
func (h Dnstap) ServeDNS(ctx context.Context, w dns.ResponseWriter, r *dns.Msg) (int, error) {
	rw := &ResponseWriter{
		ResponseWriter: w,
		Dnstap:         h,
		query:          r,
		queryTime:      time.Now(),
	}
	h.state = request.Request{W: w, Req: r}
	h.ctx = ctx
	h.rrw = dnstest.NewRecorder(w)

	// The query tap message should be sent before sending the query to the
	// forwarder. Otherwise, the tap messages will come out out of order.
	h.tapQuery(w, r, rw.queryTime)

	return plugin.NextOrFailure(h.Name(), h.Next, ctx, rw, r)
}

// Name implements the plugin.Plugin interface.
func (h Dnstap) Name() string { return "dnstap" }
