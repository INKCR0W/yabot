package telegram

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/url"
	"strings"
	"testing"
	"testing/iotest"
	"time"
)

const testToken = "123456:fake-YABOT-test-credential_X9k"

func newTestClient(t *testing.T, handler http.HandlerFunc, log *slog.Logger) (*Client, *TimeSkew) {
	t.Helper()
	server := newBotTestServer(t, handler)
	t.Cleanup(server.Close)
	skew := NewTimeSkew(log, nil)
	client, err := New(Config{Token: testToken, ServerURL: server.URL, Skew: skew, Log: log})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(client.http.CloseIdleConnections)
	return client, skew
}

func writeTestResponse(t *testing.T, w http.ResponseWriter, body string) {
	t.Helper()
	if _, err := io.WriteString(w, body); err != nil {
		t.Error(err)
	}
}

func TestNew(t *testing.T) {
	for _, tc := range []struct {
		name   string
		change func(*Config)
	}{
		{"token", func(c *Config) { c.Token = "" }},
		{"server URL", func(c *Config) { c.ServerURL = "" }},
		{"skew", func(c *Config) { c.Skew = nil }},
		{"invalid server URL", func(c *Config) { c.ServerURL = "https://[" + testToken }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cfg := Config{Token: testToken, ServerURL: "http://127.0.0.1:1", Skew: NewTimeSkew(nil, nil)}
			tc.change(&cfg)
			client, err := New(cfg)
			if client != nil || err == nil || strings.Contains(err.Error(), testToken) {
				t.Fatalf("New = (%p, %v), want a safe configuration error", client, err)
			}
		})
	}
	client, _ := newTestClient(t, func(http.ResponseWriter, *http.Request) {
		t.Error("New made a Bot API request")
	}, nil)
	transport := client.http.Transport.(*skewTransport).Transport
	defaults := http.DefaultTransport.(*http.Transport)
	if client.http.Timeout != 0 || transport.MaxIdleConnsPerHost != 32 || transport.IdleConnTimeout != 90*time.Second ||
		!transport.ForceAttemptHTTP2 || transport.TLSHandshakeTimeout != 5*time.Second {
		t.Errorf("unexpected HTTP timeout or transport settings: timeout=%s, transport=%+v", client.http.Timeout, transport)
	}
	if transport == defaults || transport.Proxy == nil || transport.DialContext == nil || transport.MaxIdleConns != defaults.MaxIdleConns {
		t.Error("transport did not preserve cloned defaults")
	}
}

func TestTransportObservesBothCallPaths(t *testing.T) {
	for _, tc := range []struct {
		name       string
		useLibrary bool
		status     int
	}{
		{"library success", true, 200},
		{"raw success", false, 200},
		{"library failure", true, 401},
		{"raw failure", false, 401},
	} {
		t.Run(tc.name, func(t *testing.T) {
			client, skew := newTestClient(t, func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("Date", time.Now().Add(-7*time.Second).UTC().Format(http.TimeFormat))
				w.WriteHeader(tc.status)
				if tc.status == 401 {
					writeTestResponse(t, w, `{"ok":false,"error_code":401,"description":"Unauthorized"}`)
					return
				}
				writeTestResponse(t, w, `{"ok":true,"result":{"id":123456,"username":"test_bot"}}`)
			}, nil)
			var err error
			if tc.useLibrary {
				_, err = client.Bot.GetMe(t.Context())
			} else {
				_, err = client.GetMe(t.Context())
			}
			if (err != nil) != (tc.status != 200) {
				t.Fatalf("GetMe error = %v, status = %d", err, tc.status)
			}
			if got := skew.Skew(); got < 6*time.Second || got > 8*time.Second {
				t.Errorf("Skew = %s, want 7s ±1s", got)
			}
		})
	}
}

func TestGetUpdatesUsesSharedConnectionWithoutAffectingSkew(t *testing.T) {
	addresses := make(chan string, 6)
	client, skew := newTestClient(t, func(w http.ResponseWriter, r *http.Request) {
		select {
		case addresses <- r.RemoteAddr:
		default:
			t.Error("unexpected extra Bot API request")
		}
		if strings.HasSuffix(r.URL.Path, "/getUpdates") {
			w.Header().Set("Date", time.Now().Add(-40*time.Second).UTC().Format(http.TimeFormat))
			writeTestResponse(t, w, `{"ok":true,"result":[]}`)
			return
		}
		w.Header().Set("Date", time.Now().Add(-7*time.Second).UTC().Format(http.TimeFormat))
		writeTestResponse(t, w, `{"ok":true,"result":{"id":123456}}`)
	}, nil)
	if _, err := client.Bot.GetMe(t.Context()); err != nil {
		t.Fatal(err)
	}
	before := skew.Skew()
	if before == 0 {
		t.Fatal("expected the library getMe to contribute a sample")
	}
	for i := range 5 {
		if _, err := client.GetUpdates(t.Context(), int64(i), 100, 25*time.Second, nil); err != nil {
			t.Fatal(err)
		}
	}
	if got := skew.Skew(); got != before {
		t.Errorf("getUpdates changed skew from %s to %s", before, got)
	}
	if len(addresses) != cap(addresses) {
		t.Fatalf("received %d requests, want %d", len(addresses), cap(addresses))
	}
	addr := <-addresses
	for range 5 {
		if got := <-addresses; got != addr {
			t.Errorf("getUpdates connection = %s, library connection = %s", got, addr)
		}
	}
}

func TestBotErrorDoesNotExposeToken(t *testing.T) {
	for _, tc := range []struct {
		name, body string
		status     int
		want       Class
	}{
		{"malformed redirect", "", 302, Retry},
		{"gateway body", `Gateway error (\upstream) for /bot` + testToken + "/getMe", 502, Retry},
		{"malformed gateway JSON", `{"description":"upstream debug info for ` + testToken, 502, Retry},
	} {
		t.Run(tc.name, func(t *testing.T) {
			client, _ := newTestClient(t, func(w http.ResponseWriter, r *http.Request) {
				if tc.status == http.StatusFound {
					w.Header().Set("Location", "/bot"+testToken+"/getMe%ZZ")
				}
				w.WriteHeader(tc.status)
				writeTestResponse(t, w, tc.body)
			}, nil)
			_, err := client.Bot.GetMe(t.Context())
			if err == nil {
				t.Fatal("GetMe succeeded, want an error")
			}
			for cause := err; cause != nil; cause = errors.Unwrap(cause) {
				if strings.Contains(cause.Error(), testToken) || strings.Contains(cause.Error(), url.QueryEscape(testToken)) {
					t.Errorf("GetMe exposed the token in an error of type %T", cause)
				}
			}
			if got := Classify(err); got.Class != tc.want {
				t.Errorf("Classify = %+v, want %s (%v)", got, tc.want, err)
			}
		})
	}
}

func TestRoundTripPreservesTransportFailure(t *testing.T) {
	cause := errors.New("dial failed for " + testToken)
	transport := &skewTransport{
		Transport: &http.Transport{DialContext: func(context.Context, string, string) (net.Conn, error) {
			return nil, cause
		}},
		token: testToken,
	}
	req, err := http.NewRequestWithContext(t.Context(), http.MethodPost, "http://example.invalid", nil)
	if err != nil {
		t.Fatal(err)
	}
	resp, err := transport.RoundTrip(req)
	if resp != nil || !errors.Is(err, cause) || Classify(err).Class != Retry {
		t.Fatal("expected a retryable transport error preserving the dial failure")
	}
	if strings.Contains(err.Error(), testToken) {
		t.Error("transport error exposed the token")
	}
}

func TestReadBodyPreservesTransportFailures(t *testing.T) {
	readErr := fmt.Errorf("read response for %s: %w", testToken, context.DeadlineExceeded)
	closeErr := &Error{Outcome: Outcome{Class: Fatal}, Err: errors.New("close response failed")}
	for _, tc := range []struct {
		name     string
		reader   io.Reader
		readErr  error
		closeErr error
		want     Class
	}{
		{"end of stream", strings.NewReader("body"), nil, nil, OK},
		{"read failure", iotest.ErrReader(readErr), readErr, nil, Retry},
		{"close failure", strings.NewReader("body"), nil, closeErr, Retry},
		{"read and close failure", iotest.ErrReader(readErr), readErr, closeErr, Retry},
	} {
		t.Run(tc.name, func(t *testing.T) {
			resp := &http.Response{Body: &transportResponseBody{
				ReadCloser: responseBodyWithCloseError{Reader: tc.reader, closeErr: tc.closeErr},
				token:      testToken,
			}}
			body, err := readBody(resp)
			if got := Classify(err).Class; got != tc.want {
				t.Fatalf("Classify(%v) = %s, want %s", err, got, tc.want)
			}
			if err == nil {
				if string(body) != "body" {
					t.Errorf("response body = %q, want body", body)
				}
				return
			}
			if strings.Contains(err.Error(), testToken) {
				t.Error("response body error exposed the token")
			}
			if tc.readErr != nil && (!errors.Is(err, tc.readErr) || !errors.Is(err, context.DeadlineExceeded)) {
				t.Errorf("response body error %v lost the read failure", err)
			}
			if tc.closeErr != nil && !errors.Is(err, tc.closeErr) {
				t.Errorf("response body error %v lost the close failure", err)
			}
		})
	}
}

func TestLimitedResponseBody(t *testing.T) {
	for _, tc := range []struct {
		name      string
		reader    io.Reader
		want      string
		oversized bool
	}{
		{"under limit", strings.NewReader("abc"), "abc", false},
		{"at limit", strings.NewReader("abcd"), "abcd", false},
		{"at limit with data and EOF", iotest.DataErrReader(strings.NewReader("abcd")), "abcd", false},
		{"over limit", strings.NewReader("abcde"), "abcd", true},
		{"over limit with data and EOF", iotest.DataErrReader(strings.NewReader("abcde")), "abcd", true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			body := &limitedResponseBody{ReadCloser: io.NopCloser(tc.reader), remaining: 4}
			got, err := io.ReadAll(body)
			if string(got) != tc.want {
				t.Errorf("body = %q, want %q", got, tc.want)
			}
			if tc.oversized {
				if err == nil || !strings.Contains(err.Error(), "exceeds 64 MiB") {
					t.Errorf("read error = %v, want a size limit error", err)
				}
			} else if err != nil {
				t.Errorf("read error = %v, want nil", err)
			}
		})
	}
}

type responseBodyWithCloseError struct {
	io.Reader
	closeErr error
}

func (b responseBodyWithCloseError) Close() error { return b.closeErr }
