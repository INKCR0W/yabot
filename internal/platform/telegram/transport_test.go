package telegram

import (
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func TestResponseBodyErrorsAreRetryable(t *testing.T) {
	for _, call := range []struct {
		name string
		do   func(context.Context, *Client) error
	}{
		{"GetUpdates", func(ctx context.Context, c *Client) error {
			_, err := c.GetUpdates(ctx, 0, 1, 0, nil)
			return err
		}},
		{"Bot.GetMe", func(ctx context.Context, c *Client) error {
			_, err := c.Bot.GetMe(ctx)
			return err
		}},
	} {
		for _, failure := range []struct {
			name       string
			start      func(*testing.T, http.Handler) *httptest.Server
			protoMajor int
			reset      bool
		}{
			{"HTTP2 stream reset", newHTTP2TestServer, 2, true},
			{"HTTP1 cancel with cause", newBotTestServer, 1, false},
		} {
			t.Run(call.name+"/"+failure.name, func(t *testing.T) {
				ctx, stop := context.WithTimeout(t.Context(), 5*time.Second)
				defer stop()
				ctx, cancel := context.WithCancelCause(ctx)
				defer cancel(nil)
				cause := errors.New("stop after partial response for " + testToken)
				bodyRead := make(chan struct{})
				handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					if r.ProtoMajor != failure.protoMajor {
						t.Errorf("request protocol = %s, want HTTP/%d", r.Proto, failure.protoMajor)
						return
					}
					w.WriteHeader(http.StatusOK)
					if _, err := io.WriteString(w, `{"ok":true,"result":`); err != nil {
						t.Errorf("write partial response: %v", err)
						return
					}
					if err := http.NewResponseController(w).Flush(); err != nil {
						t.Errorf("flush partial response: %v", err)
						return
					}
					if failure.reset {
						select {
						case <-bodyRead:
							panic(http.ErrAbortHandler)
						case <-r.Context().Done():
						}
						return
					}
					<-r.Context().Done()
				})
				server := failure.start(t, handler)
				client := newResponseBodyTestClient(t, server, func() {
					close(bodyRead)
					if !failure.reset {
						cancel(cause)
					}
				})
				err := call.do(ctx, client)
				select {
				case <-bodyRead:
				default:
					t.Fatalf("failed before reading partial response: %v", err)
				}
				if err == nil {
					t.Fatal("expected an error reading the partial response")
				}
				if failure.reset && !strings.Contains(err.Error(), "INTERNAL_ERROR; received from peer") {
					t.Fatalf("error = %v, want an HTTP/2 peer stream reset", err)
				}
				if !failure.reset && (!errors.Is(err, cause) || strings.Contains(err.Error(), testToken)) {
					t.Fatal("expected a redacted error preserving the cancellation cause")
				}
				if got := Classify(err); got.Class != Retry {
					t.Errorf("Classify(%v) = %s, want %s", err, got.Class, Retry)
				}
			})
		}
	}
}

func TestBotHTTPClientStreamsResponse(t *testing.T) {
	for _, tc := range []struct {
		name          string
		contentLength string
		oversized     bool
	}{
		{"normal response", "4", false},
		{"declared oversized response", "67108865", true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			server := newBotTestServer(t, http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				w.Header().Set("Content-Length", tc.contentLength)
				writeTestResponse(t, w, "body")
			}))
			read := false
			client := newResponseBodyTestClient(t, server, func() { read = true })
			req, err := http.NewRequestWithContext(t.Context(), http.MethodPost, server.URL+"/getMe", nil)
			if err != nil {
				t.Fatal(err)
			}
			resp, err := (botHTTPClient{client: client}).Do(req)
			if err != nil {
				t.Fatal(err)
			}
			defer func() {
				if err := resp.Body.Close(); err != nil {
					t.Error(err)
				}
			}()
			if read {
				t.Error("Do read the response body before returning it")
			}
			body, err := io.ReadAll(resp.Body)
			if tc.oversized {
				if read || len(body) != 0 || err == nil || !strings.Contains(err.Error(), "exceeds 64 MiB") || Classify(err).Class != Fatal {
					t.Errorf("read oversized response = (%q, %v), want a fatal size limit error without reading the server body", body, err)
				}
			} else if !read || string(body) != "body" || err != nil {
				t.Errorf("read response = (%q, %v), want body", body, err)
			}
		})
	}
}

func newHTTP2TestServer(t *testing.T, handler http.Handler) *httptest.Server {
	t.Helper()
	server := httptest.NewUnstartedServer(handler)
	server.EnableHTTP2 = true
	server.StartTLS()
	return server
}

func newResponseBodyTestClient(t *testing.T, server *httptest.Server, onBodyRead func()) *Client {
	t.Helper()
	t.Cleanup(server.Close)
	client, err := New(Config{Token: testToken, ServerURL: server.URL, Skew: NewTimeSkew(nil, nil)})
	if err != nil {
		t.Fatal(err)
	}
	transport := client.http.Transport.(*skewTransport)
	if server.TLS != nil {
		transport.Transport.TLSClientConfig = server.Client().Transport.(*http.Transport).TLSClientConfig.Clone()
	}
	t.Cleanup(transport.CloseIdleConnections)
	client.http.Transport = bodyReadObserver{RoundTripper: transport, onBodyRead: onBodyRead}
	return client
}

type bodyReadObserver struct {
	http.RoundTripper
	onBodyRead func()
}

func (t bodyReadObserver) RoundTrip(req *http.Request) (*http.Response, error) {
	resp, err := t.RoundTripper.RoundTrip(req)
	if err != nil {
		return nil, err
	}
	resp.Body = &observedBody{ReadCloser: resp.Body, onBodyRead: t.onBodyRead}
	return resp, nil
}

type observedBody struct {
	io.ReadCloser
	onBodyRead func()
}

func (b *observedBody) Read(p []byte) (int, error) {
	n, err := b.ReadCloser.Read(p)
	if n > 0 && b.onBodyRead != nil {
		b.onBodyRead()
		b.onBodyRead = nil
	}
	return n, err
}
