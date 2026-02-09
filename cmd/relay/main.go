package main

import (
	"flag"
	"fmt"
	"os"
	"text/tabwriter"

	"github.com/MohamedAklamaash/relay"
)

func main() {
	addr := flag.String("addr", "127.0.0.1:6379", "redis address")
	db := flag.Int("db", 0, "redis db")
	password := flag.String("password", "", "redis password")
	flag.Parse()

	args := flag.Args()
	if len(args) == 0 {
		usage()
		os.Exit(2)
	}

	insp := relay.NewInspector(relay.RedisClientOpt{Addr: *addr, DB: *db, Password: *password})
	defer insp.Close()

	var err error
	switch args[0] {
	case "stats":
		err = runStats(insp)
	case "ls":
		err = runList(insp, args[1:])
	case "rm":
		err = runDelete(insp, args[1:])
	case "run":
		err = runTask(insp, args[1:])
	case "cancel":
		err = runCancel(insp, args[1:])
	case "pause":
		err = runPause(insp, args[1:], true)
	case "unpause":
		err = runPause(insp, args[1:], false)
	default:
		usage()
		os.Exit(2)
	}

	if err != nil {
		fmt.Fprintln(os.Stderr, "error:", err)
		os.Exit(1)
	}
}

func usage() {
	fmt.Fprintln(os.Stderr, `relay - inspect and manage queues

usage:
  relay [flags] stats
  relay [flags] ls <queue> <state>
  relay [flags] rm <queue> <id>
  relay [flags] run <queue> <id>
  relay [flags] cancel <id>
  relay [flags] pause <queue>
  relay [flags] unpause <queue>

states: pending active scheduled retry archived completed

flags:
  -addr      redis address (default 127.0.0.1:6379)
  -db        redis db
  -password  redis password`)
}

func runStats(insp *relay.Inspector) error {
	queues, err := insp.Queues()
	if err != nil {
		return err
	}
	w := tabwriter.NewWriter(os.Stdout, 0, 4, 2, ' ', 0)
	fmt.Fprintln(w, "QUEUE\tPENDING\tACTIVE\tSCHEDULED\tRETRY\tARCHIVED\tCOMPLETED\tPAUSED")
	for _, q := range queues {
		s, err := insp.Stats(q)
		if err != nil {
			return err
		}
		fmt.Fprintf(w, "%s\t%d\t%d\t%d\t%d\t%d\t%d\t%v\n",
			s.Queue, s.Pending, s.Active, s.Scheduled, s.Retry, s.Archived, s.Completed, s.Paused)
	}
	return w.Flush()
}

func runList(insp *relay.Inspector, args []string) error {
	if len(args) < 2 {
		return fmt.Errorf("usage: ls <queue> <state>")
	}
	tasks, err := insp.ListTasks(args[0], args[1], 50)
	if err != nil {
		return err
	}
	w := tabwriter.NewWriter(os.Stdout, 0, 4, 2, ' ', 0)
	fmt.Fprintln(w, "ID\tTYPE\tRETRIED\tMAXRETRY\tLASTERR")
	for _, t := range tasks {
		fmt.Fprintf(w, "%s\t%s\t%d\t%d\t%s\n", t.ID, t.Type, t.Retried, t.MaxRetry, t.LastErr)
	}
	return w.Flush()
}

func runDelete(insp *relay.Inspector, args []string) error {
	if len(args) < 2 {
		return fmt.Errorf("usage: rm <queue> <id>")
	}
	return insp.DeleteTask(args[0], args[1])
}

func runTask(insp *relay.Inspector, args []string) error {
	if len(args) < 2 {
		return fmt.Errorf("usage: run <queue> <id>")
	}
	return insp.RunTask(args[0], args[1])
}

func runCancel(insp *relay.Inspector, args []string) error {
	if len(args) < 1 {
		return fmt.Errorf("usage: cancel <id>")
	}
	return insp.CancelTask(args[0])
}

func runPause(insp *relay.Inspector, args []string, pause bool) error {
	if len(args) < 1 {
		return fmt.Errorf("usage: pause/unpause <queue>")
	}
	if pause {
		return insp.PauseQueue(args[0])
	}
	return insp.UnpauseQueue(args[0])
}
