package main

import (
	"context"
	"fmt"
	"os"

	"github.com/c-14/gtimelog/cmd"
)

const version = "0.1.0-alpha"

func usage() string {
	return `usage: gtimelog [--help] {store|analyze} ...

Subcommands:
	store <output_db>
	analyze <database> [-s <date>] [-e <date>]
	`
}

func main() {
	if len(os.Args) < 2 {
		fmt.Fprintln(os.Stderr, usage())
		os.Exit(EX_USAGE)
	}
	var err error
	ctx := context.Background()

	switch command := os.Args[1]; command {
	case "store":
		err = cmd.Store(ctx, os.Args[2:])
	case "analyze":
		err = cmd.Analyze(ctx, os.Args[2:])
	case "-v":
		fallthrough
	case "--version":
		fmt.Println(version)
	case "-h":
		fallthrough
	case "--help":
		fmt.Println(usage())
	default:
		fmt.Fprintln(os.Stderr, usage())
		os.Exit(EX_USAGE)
	}
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
	}
}
