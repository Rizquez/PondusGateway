package main

import (
	"bytes"
	"context"
	"errors"
	"strings"
	"testing"
)

func TestParseConfig(t *testing.T) {
	tests := []struct {
		name      string
		arguments []string
		wantError bool
	}{
		{name: "default configuration", arguments: nil},
		{name: "guarded scheduler", arguments: []string{"-scheduler", "capacity-fit-guarded", "-max-bypasses", "3"}},
		{name: "negative read timeout", arguments: []string{"-read-timeout", "-1s"}, wantError: true},
		{name: "unknown scheduler", arguments: []string{"-scheduler", "unknown"}, wantError: true},
		{name: "guard without bypasses", arguments: []string{"-scheduler", "capacity-fit-guarded"}, wantError: true},
		{name: "bypasses without guard", arguments: []string{"-max-bypasses", "2"}, wantError: true},
		{name: "invalid concurrency", arguments: []string{"-concurrency", "0"}, wantError: true},
		{name: "invalid duration", arguments: []string{"-duration", "0s"}, wantError: true},
		{name: "unexpected positional argument", arguments: []string{"unexpected"}, wantError: true},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			config, err := parseConfig(test.arguments)
			if test.wantError {
				if err == nil {
					t.Fatal("parseConfig() error = nil; want an error")
				}

				return
			}

			if err != nil {
				t.Fatalf("parseConfig() = %v; want valid configuration", err)
			}

			if config.transport.ReadTimeout != 0 {
				t.Fatalf("ReadTimeout = %s; want disabled (0)", config.transport.ReadTimeout)
			}
		})
	}
}

func TestServeClosesOnContextCancellation(t *testing.T) {
	config, err := parseConfig([]string{"-addr", "127.0.0.1:0", "-duration", "10ms"})
	if err != nil {
		t.Fatalf("parseConfig() = %v; want nil", err)
	}

	serverContext, cancelServer := context.WithCancel(context.Background())
	cancelServer()

	var output bytes.Buffer
	serveError := serve(serverContext, config, &output)
	if serveError != nil && !errors.Is(serveError, context.Canceled) {
		t.Fatalf("serve() = %v; want nil or context cancellation", serveError)
	}

	if !strings.Contains(output.String(), "Pondus stream TCP listening on") {
		t.Fatalf("serve() output = %q; want listening announcement", output.String())
	}
}
