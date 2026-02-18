package main

import (
	"log"

	"github.com/MohamedAklamaash/relay"
)

func main() {
	scheduler := relay.NewScheduler(relay.RedisClientOpt{Addr: "127.0.0.1:6379"}, nil)

	if _, err := scheduler.Register("*/1 * * * *", relay.NewTask("report:build", []byte(`{"kind":"minutely"}`))); err != nil {
		log.Fatal(err)
	}
	if _, err := scheduler.Register("0 * * * *", relay.NewTask("email:send", []byte(`{"to":"ops@example.com","subject":"hourly"}`)), relay.Queue("critical")); err != nil {
		log.Fatal(err)
	}

	if err := scheduler.Run(); err != nil {
		log.Fatal(err)
	}
}
