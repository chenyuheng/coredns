package dnstap

import (
	"context"
	"net"
	"testing"

	"github.com/coredns/coredns/plugin/dnstap/msg"
	test "github.com/coredns/coredns/plugin/test"
	"github.com/coredns/coredns/plugin/metadata"

	tap "github.com/dnstap/golang-dnstap"
	"github.com/miekg/dns"
)

func testCase(t *testing.T, tapq, tapr *tap.Dnstap, q, r *dns.Msg) {
	w := writer{t: t}
	w.queue = append(w.queue, tapq, tapr)
	h := Dnstap{
		Next: test.HandlerFunc(func(_ context.Context,
			w dns.ResponseWriter, _ *dns.Msg) (int, error) {
			return 0, w.WriteMsg(r)
		}),
		io: &w,
		ExtraFormat: "extra_field_{/metadata/test}_{type}_{name}_{class}_{proto}_{size}_{remote}_{port}_{local}",
	}
	ctx := metadata.ContextWithMetadata(context.TODO())
	ok := metadata.SetValueFunc(ctx, "metadata/test", func() string {
		return "MetadataValue"
	})
	if !ok {
		t.Fatal("Failed to set metadata")
	}
	_, err := h.ServeDNS(ctx, &test.ResponseWriter{}, q)
	if err != nil {
		t.Fatal(err)
	}
}

type writer struct {
	t     *testing.T
	queue []*tap.Dnstap
}

func (w *writer) Dnstap(e *tap.Dnstap) {
	if len(w.queue) == 0 {
		w.t.Error("Message not expected")
	}

	ex := w.queue[0].Message
	got := e.Message

	if string(ex.QueryAddress) != string(got.QueryAddress) {
		w.t.Errorf("Expected source address %s, got %s", ex.QueryAddress, got.QueryAddress)
	}
	if string(ex.ResponseAddress) != string(got.ResponseAddress) {
		w.t.Errorf("Expected response address %s, got %s", ex.ResponseAddress, got.ResponseAddress)
	}
	if *ex.QueryPort != *got.QueryPort {
		w.t.Errorf("Expected port %d, got %d", *ex.QueryPort, *got.QueryPort)
	}
	if *ex.SocketFamily != *got.SocketFamily {
		w.t.Errorf("Expected socket family %d, got %d", *ex.SocketFamily, *got.SocketFamily)
	}
	if string(w.queue[0].Extra) != string(e.Extra) {
		w.t.Errorf("Expected extra %s, got %s", w.queue[0].Extra, e.Extra)
	}
	w.queue = w.queue[1:]
}

func TestDnstap(t *testing.T) {
	q := test.Case{Qname: "example.org", Qtype: dns.TypeA}.Msg()
	r := test.Case{
		Qname: "example.org.", Qtype: dns.TypeA,
		Answer: []dns.RR{
			test.A("example.org. 3600	IN	A 10.0.0.1"),
		},
	}.Msg()
	tapq := &tap.Dnstap {
		Message: testMessage(), // leave type unset for deepEqual
		Extra: []byte("extra_field_MetadataValue_A_example.org._IN_udp_29_10.240.0.1_40212_127.0.0.1"),
	}
	msg.SetType(tapq.Message, tap.Message_CLIENT_QUERY)
	tapr := &tap.Dnstap {
		Message: testMessage(),
		Extra: []byte("extra_field_MetadataValue_A_example.org._IN_udp_29_10.240.0.1_40212_127.0.0.1"),
	}
	msg.SetType(tapr.Message, tap.Message_CLIENT_RESPONSE)
	testCase(t, tapq, tapr, q, r)
}

func testMessage() *tap.Message {
	inet := tap.SocketFamily_INET
	udp := tap.SocketProtocol_UDP
	port := uint32(40212)
	return &tap.Message{
		SocketFamily:   &inet,
		SocketProtocol: &udp,
		QueryAddress:   net.ParseIP("10.240.0.1"),
		QueryPort:      &port,
	}
}

func TestTapMessage(t *testing.T) {
	tapq := &tap.Dnstap {
		Message: testMessage(),
		// extra field would be replaced, since TapMessage does not pass context
		Extra: []byte("extra_field_{/metadata/test}_{type}_{name}_{class}_{proto}_{size}_{remote}_{port}_{local}"),
	}
	msg.SetType(tapq.Message, tap.Message_CLIENT_QUERY)

	w := writer{t: t}
	w.queue = append(w.queue, tapq)
	h := Dnstap{
		Next: test.HandlerFunc(func(_ context.Context,
			w dns.ResponseWriter, r *dns.Msg) (int, error) {
			return 0, w.WriteMsg(r)
		}),
		io: &w,
		ExtraFormat: "extra_field_{/metadata/test}_{type}_{name}_{class}_{proto}_{size}_{remote}_{port}_{local}",
	}
	h.TapMessage(tapq.Message)
}