package main

import (
	"context"
	"encoding/json"
	"log"
	"net/http"

	"github.com/MohamedAklamaash/relay"
)

type EmailPayload struct {
	To      string `json:"to"`
	Subject string `json:"subject"`
}

func main() {
	srv := relay.NewServer(relay.RedisClientOpt{Addr: "127.0.0.1:6379"}, relay.Config{
		Concurrency: 10,
		Queues:      map[string]int{"critical": 6, "default": 3, "low": 1},
	})

	mux := relay.NewServeMux()
	mux.HandleFunc("email:send", handleEmail)
	mux.HandleFunc("report:build", handleReport)

	http.Handle("/metrics", relay.MetricsHandler())
	go http.ListenAndServe(":2112", nil)

	if err := srv.Run(mux); err != nil {
		log.Fatal(err)
	}
}

func handleEmail(ctx context.Context, t *relay.Task) error {
	var p EmailPayload
	if err := json.Unmarshal(t.Payload(), &p); err != nil {
		return err
	}
	log.Printf("sending email to %s: %s", p.To, p.Subject)
	return nil
}

func handleReport(ctx context.Context, t *relay.Task) error {
	log.Printf("building report: %s", string(t.Payload()))
	return nil
}
