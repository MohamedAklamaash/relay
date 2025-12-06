package relay

import (
	"crypto/tls"

	"github.com/redis/go-redis/v9"
)

type RedisConnOpt interface {
	MakeClient() redis.UniversalClient
}

type RedisClientOpt struct {
	Network   string
	Addr      string
	Username  string
	Password  string
	DB        int
	PoolSize  int
	TLSConfig *tls.Config
}

func (o RedisClientOpt) MakeClient() redis.UniversalClient {
	network := o.Network
	if network == "" {
		network = "tcp"
	}
	addr := o.Addr
	if addr == "" {
		addr = "127.0.0.1:6379"
	}
	return redis.NewClient(&redis.Options{
		Network:   network,
		Addr:      addr,
		Username:  o.Username,
		Password:  o.Password,
		DB:        o.DB,
		PoolSize:  o.PoolSize,
		TLSConfig: o.TLSConfig,
	})
}

type clientWrapper struct {
	client redis.UniversalClient
}

func (w clientWrapper) MakeClient() redis.UniversalClient {
	return w.client
}

func wrapClient(c redis.UniversalClient) RedisConnOpt {
	return clientWrapper{client: c}
}
