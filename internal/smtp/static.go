package smtp

import (
	"context"
	"net"
)

// StaticRoute returns a resolver and dialer that send mail for every domain
// to one address, e.g. a local fake server. For development and tests only.
func StaticRoute(addr string) (Resolver, Dialer) {
	return staticResolver{}, staticDialer{addr: addr}
}

const staticHost = "static-mx.mailq.invalid"

type staticResolver struct{}

func (staticResolver) LookupMX(context.Context, string) ([]*net.MX, error) {
	return []*net.MX{{Host: staticHost + ".", Pref: 10}}, nil
}

func (staticResolver) LookupHost(context.Context, string) ([]string, error) {
	return []string{"127.0.0.1"}, nil
}

type staticDialer struct{ addr string }

func (d staticDialer) DialContext(ctx context.Context, network, _ string) (net.Conn, error) {
	var nd net.Dialer
	return nd.DialContext(ctx, network, d.addr)
}
