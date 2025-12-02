package main

import (
	"os"

	"cksr/cmd"
)

func main() {
	if err := cmd.NewRootCmd().Execute(); err != nil {
		os.Exit(cmd.ResolveExitCode(err))
	}
}
