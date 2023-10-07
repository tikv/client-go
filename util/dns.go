package util

import (
	"context"
	"fmt"
	"net"
	"strings"
	"time"
)

type dnsF func(ctx context.Context, target string) (net.Conn, error)

func wrapWithDomain(target, domain string) (string, error) {
	if len(domain) == 0 {
		return target, nil
	}
	strlist := strings.Split(target, ":")
	if len(strlist) != 2 {
		return "", fmt.Errorf("target %s is not valid", target)
	}
	address := strlist[0]
	port := strlist[1]
	return fmt.Sprintf("%s.%s:%s", address, domain, port), nil
}

// GetDefaultDNSDialer is a util which built for connecting TiKV on k8s.
// Here is an example:
// coreDNSAddr := "8.8.8.8:53"
// domain := "cluster.local"
// dialer := grpc.WithContextDialer(util.GetCustomDNSDialer(coreDNSAddr, domain))
// cli, err := rawkv.NewClientWithOpts(
//
//	context.TODO(),
//	[]string{"pd0.pd:2379"},
//	rawkv.WithPDOptions(pd.WithGRPCDialOptions(dialer)),
//	rawkv.WithGRPCDialOptions(dialer),
//
// )
// cli.Close()
func GetCustomDNSDialer(dnsServer, dnsDomain string) dnsF {
	return func(ctx context.Context, target string) (net.Conn, error) {
		r := net.Resolver{
			PreferGo: true,
			Dial: func(ctx context.Context, network, _ string) (net.Conn, error) {
				d := net.Dialer{
					Timeout: time.Millisecond * time.Duration(10000),
				}
				return d.DialContext(ctx, network, dnsServer)
			},
		}
		dialer := &net.Dialer{
			Resolver: &r,
		}
		addr, err := wrapWithDomain(target, dnsDomain)
		if err != nil {
			return nil, err
		}
		return dialer.DialContext(ctx, "tcp", addr)
	}
}
