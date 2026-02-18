package main

import (
	"encoding/json"
	"log"
	"time"

	"github.com/MohamedAklamaash/relay"
)

type EmailPayload struct {
	To      string `json:"to"`
	Subject string `json:"subject"`
}

func main() {
	client := relay.NewClient(relay.RedisClientOpt{Addr: "127.0.0.1:6379"})
	defer client.Close()

	payload, _ := json.Marshal(EmailPayload{To: "user@example.com", Subject: "welcome"})

	now, err := client.Enqueue(relay.NewTask("email:send", payload), relay.Queue("critical"))
	if err != nil {
		log.Fatal(err)
	}
	log.Printf("enqueued immediate task %s", now.ID)

	later, err := client.Enqueue(
		relay.NewTask("report:build", []byte(`{"month":"june"}`)),
		relay.ProcessIn(10*time.Second),
		relay.MaxRetry(5),
	)
	if err != nil {
		log.Fatal(err)
	}
	log.Printf("enqueued delayed task %s for %s", later.ID, later.NextProcessAt.Format(time.RFC3339))

	_, err = client.Enqueue(
		relay.NewTask("email:send", payload),
		relay.Queue("critical"),
		relay.Unique(time.Minute),
	)
	if err != nil {
		log.Printf("second unique enqueue rejected as expected: %v", err)
	}
}
