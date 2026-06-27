package main

import (
	"context"
	"log"
	"net/http"
	"os"

	"github.com/MohamedAklamaash/relay"
)

func main() {
	addr := os.Getenv("REDIS_ADDR")
	if addr == "" {
		addr = "127.0.0.1:6379"
	}

	srv := relay.NewServer(relay.RedisClientOpt{Addr: addr}, relay.Config{
		Concurrency: 20,
		Queues:      map[string]int{"critical": 6, "default": 3, "low": 1},
	})

	mux := relay.NewServeMux()
	mux.HandleFunc("email:send", handleEmail)
	mux.HandleFunc("report:build", handleReport)

	go func() {
		http.Handle("/metrics", relay.MetricsHandler())
		log.Fatal(http.ListenAndServe(":2112", nil))
	}()

	if err := srv.Run(mux); err != nil {
		log.Fatal(err)
	}
}

func handleEmail(ctx context.Context, t *relay.Task) error {
	log.Printf("email -> %s", t.Payload())
	return nil
}

func handleReport(ctx context.Context, t *relay.Task) error {
	log.Printf("report -> %s", t.Payload())
	return nil
}
