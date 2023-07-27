package forward

import (
	"net"
	"strconv"
	"time"

	"github.com/coredns/coredns/plugin/dnstap/msg"
	"github.com/coredns/coredns/plugin/pkg/proxy"
	"github.com/coredns/coredns/request"

	tap "github.com/dnstap/golang-dnstap"
	"github.com/miekg/dns"

	"fmt"
)

// toDnstap will send the forward and received message to the dnstap plugin.
func toDnstap(f *Forward, host string, state request.Request, opts proxy.Options, reply *dns.Msg, start time.Time) {
	fmt.Println("forward toDnstap 1")
	h, p, _ := net.SplitHostPort(host)      // this is preparsed and can't err here
	port, _ := strconv.ParseUint(p, 10, 32) // same here
	ip := net.ParseIP(h)

	var ta net.Addr = &net.UDPAddr{IP: ip, Port: int(port)}
	t := state.Proto()
	switch {
	case opts.ForceTCP:
		t = "tcp"
	case opts.PreferUDP:
		t = "udp"
	}
	fmt.Println("forward toDnstap 2")

	if t == "tcp" {
		ta = &net.TCPAddr{IP: ip, Port: int(port)}
	}

	for _, t := range f.tapPlugins {
		fmt.Println("forward toDnstap 2.1")
		// Query
		q := new(tap.Message)
		msg.SetQueryTime(q, start)
		fmt.Println("forward toDnstap 2.2")
		// Forwarder dnstap messages are from the perspective of the downstream server
		// (upstream is the forward server)
		msg.SetQueryAddress(q, state.W.RemoteAddr())
		msg.SetResponseAddress(q, ta)
		fmt.Println("forward toDnstap 2.3")
		if t.IncludeRawMessage {
			fmt.Println("forward toDnstap 2.4")
			buf, _ := state.Req.Pack()
			q.QueryMessage = buf
		}
		fmt.Println("forward toDnstap 2.5")
		msg.SetType(q, tap.Message_FORWARDER_QUERY)
		fmt.Println("forward toDnstap 2.5.1")
		t.TapMessage(q)
		fmt.Println("forward toDnstap 2.6")
		// Response
		if reply != nil {
			r := new(tap.Message)
			if t.IncludeRawMessage {
				buf, _ := reply.Pack()
				r.ResponseMessage = buf
			}
			msg.SetQueryTime(r, start)
			msg.SetQueryAddress(r, state.W.RemoteAddr())
			msg.SetResponseAddress(r, ta)
			msg.SetResponseTime(r, time.Now())
			msg.SetType(r, tap.Message_FORWARDER_RESPONSE)
			t.TapMessage(r)
		}
	}
	fmt.Println("forward toDnstap 3")
}
