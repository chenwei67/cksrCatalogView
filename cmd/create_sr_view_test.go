package cmd

import "testing"

func TestCreateSRViewGenInheritsParentFlags(t *testing.T) {
	cmd := NewCreateSRViewCmd()
	genCmd, _, err := cmd.Find([]string{"gen"})
	if err != nil {
		t.Fatalf("find gen command failed: %v", err)
	}

	for _, flagName := range []string{"pair", "old-suffix", "new-suffix"} {
		if genCmd.InheritedFlags().Lookup(flagName) == nil {
			t.Fatalf("expected gen command to inherit flag %s", flagName)
		}
	}
	if genCmd.Flags().Lookup("output-dir") == nil {
		t.Fatal("expected gen command to have output-dir flag")
	}
}
