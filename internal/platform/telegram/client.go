package telegram

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"math"
	"net/http"
	"net/url"
	"strings"
	"time"
	"unicode"

	"github.com/go-telegram/bot"
)

type Config struct {
	Token     string
	ServerURL string
	Skew      *TimeSkew
	Log       *slog.Logger
}

type Client struct {
	Bot *bot.Bot

	http      *http.Client
	token     string
	serverURL string
	log       *slog.Logger
}

// New requires a raw token, a Bot API base URL, and a TimeSkew. A nil logger discards logs.
func New(cfg Config) (*Client, error) {
	if cfg.Token == "" {
		return nil, errors.New("telegram token is required")
	}
	if strings.ContainsAny(cfg.Token, `/\?#%`) || strings.ContainsFunc(cfg.Token, func(r rune) bool {
		return unicode.IsControl(r) || unicode.IsSpace(r)
	}) {
		return nil, errors.New("telegram token must be safe to use in a URL path")
	}
	serverURL := strings.TrimRight(cfg.ServerURL, "/")
	if serverURL == "" {
		return nil, errors.New("telegram server URL is required")
	}
	base, err := url.Parse(serverURL)
	if err != nil {
		return nil, fmt.Errorf("invalid telegram server URL: %w", redactError(err, cfg.Token))
	}
	if (base.Scheme != "http" && base.Scheme != "https") || base.Hostname() == "" ||
		base.User != nil || base.ForceQuery || base.RawQuery != "" || strings.Contains(serverURL, "#") {
		return nil, errors.New("telegram server URL must be an absolute HTTP or HTTPS URL without credentials, query, or fragment")
	}
	if cfg.Skew == nil {
		return nil, errors.New("telegram time skew estimator is required")
	}
	if cfg.Log == nil {
		cfg.Log = slog.New(slog.DiscardHandler)
	}

	defaults, ok := http.DefaultTransport.(*http.Transport)
	if !ok {
		return nil, errors.New("telegram requires an HTTP transport to clone from http.DefaultTransport")
	}
	transport := defaults.Clone()
	transport.MaxIdleConnsPerHost = 32
	transport.IdleConnTimeout = 90 * time.Second
	transport.ForceAttemptHTTP2 = true
	transport.TLSHandshakeTimeout = 5 * time.Second

	c := &Client{
		http: &http.Client{
			Transport: &skewTransport{Transport: transport, skew: cfg.Skew, token: cfg.Token},
			// A redirect can expose the token in a Referer header.
			CheckRedirect: func(*http.Request, []*http.Request) error {
				return http.ErrUseLastResponse
			},
		},
		token: cfg.Token, serverURL: serverURL, log: cfg.Log,
	}
	c.Bot, err = bot.New(cfg.Token,
		bot.WithSkipGetMe(),
		// The zero poll timeout is unused because ingress owns polling.
		bot.WithHTTPClient(0, botHTTPClient{client: c}),
		bot.WithServerURL(serverURL),
		bot.WithErrorsHandler(func(err error) {
			c.log.Error("telegram library error", "error", redactError(err, c.token))
		}),
	)
	if err != nil {
		return nil, redactError(err, cfg.Token)
	}
	return c, nil
}

type skewTransport struct {
	*http.Transport
	skew  *TimeSkew
	token string
}

func (t *skewTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	sentWall := time.Now()
	resp, err := t.Transport.RoundTrip(req)
	recvWall := time.Now()
	if err != nil {
		return nil, &transportError{cause: redactError(err, t.token)}
	}
	method := req.URL.Path[strings.LastIndexByte(req.URL.Path, '/')+1:]
	if !strings.EqualFold(method, "getUpdates") {
		t.skew.ObserveResponse(sentWall, recvWall, resp.Header.Get("Date"))
	}
	resp.Body = &transportResponseBody{ReadCloser: resp.Body, token: t.token}
	return resp, nil
}

type transportError struct{ cause error }

func (e *transportError) Error() string { return e.cause.Error() }

func (e *transportError) Unwrap() error { return e.cause }

type transportResponseBody struct {
	io.ReadCloser
	token string
}

func (b *transportResponseBody) Read(p []byte) (int, error) {
	n, err := b.ReadCloser.Read(p)
	return n, b.wrapError(err)
}

func (b *transportResponseBody) Close() error {
	return b.wrapError(b.ReadCloser.Close())
}

func (b *transportResponseBody) wrapError(err error) error {
	if err == nil || err == io.EOF {
		return err
	}
	return &transportError{cause: redactError(err, b.token)}
}

type botHTTPClient struct{ client *Client }

func (b botHTTPClient) Do(req *http.Request) (*http.Response, error) {
	c := b.client
	resp, err := c.http.Do(req)
	if err != nil {
		return nil, redactError(err, c.token)
	}
	if resp.StatusCode != http.StatusTooManyRequests && (resp.StatusCode < 500 || resp.StatusCode >= 600) {
		resp.Body = limitResponseBody(resp)
		return resp, nil
	}
	// Gateways often send non-JSON 429/5xx bodies; the library would report those as decode errors,
	// which Classify treats as Fatal, so map them by HTTP status here.
	method := req.URL.Path[strings.LastIndexByte(req.URL.Path, '/')+1:]
	body, err := readBody(resp)
	if err != nil {
		return nil, withHTTPStatusError(method, resp, err)
	}
	envelope, err := c.decodeAPIResponse(method, resp, body)
	if err != nil {
		return nil, err
	}
	if resp.StatusCode == http.StatusTooManyRequests {
		// The library cannot apply the header fallback or saturate huge values, so build its error here.
		if envelope.ErrorCode == http.StatusTooManyRequests {
			if seconds := envelope.retryAfter(resp); envelope.Parameters.RetryAfter == 0 || seconds > math.MaxInt {
				return nil, envelope.apiError(method, seconds, c.token)
			}
		}
		resp.Body = io.NopCloser(bytes.NewReader(body))
		return resp, nil
	}
	return nil, envelope.apiError(method, envelope.retryAfter(resp), c.token)
}

const (
	maxResponseBytes = 64 << 20
	responseTooLarge = "telegram response exceeds 64 MiB"
)

type limitedResponseBody struct {
	io.ReadCloser
	remaining int
}

func limitResponseBody(resp *http.Response) io.ReadCloser {
	remaining := maxResponseBytes
	if resp.ContentLength > maxResponseBytes {
		remaining = -1
	}
	return &limitedResponseBody{ReadCloser: resp.Body, remaining: remaining}
}

func (b *limitedResponseBody) Read(p []byte) (int, error) {
	if b.remaining < 0 {
		return 0, errors.New(responseTooLarge)
	}
	// Read one extra byte to distinguish a body at the limit from an oversized one.
	n, err := b.ReadCloser.Read(p[:min(len(p), b.remaining+1)])
	remaining := b.remaining
	b.remaining -= n
	if n > remaining {
		if err == nil || err == io.EOF {
			err = errors.New(responseTooLarge)
		}
		return remaining, err
	}
	return n, err
}

func tokenReplacer(token string) *strings.Replacer {
	queryToken := url.QueryEscape(token)
	return strings.NewReplacer(token, "***", url.PathEscape(token), "***", queryToken, "***", strings.ReplaceAll(queryToken, "%3A", "%3a"), "***")
}

type redactedError struct {
	message string
	cause   error
}

func (e *redactedError) Error() string { return e.message }

// Preserve error identity without exposing the unredacted cause through Unwrap.
func (e *redactedError) Is(target error) bool { return errors.Is(e.cause, target) }

func redactError(err error, token string) error {
	redact := tokenReplacer(token)
	if e, ok := err.(*url.Error); ok {
		return &url.Error{
			Op:  redact.Replace(e.Op),
			URL: redact.Replace(e.URL),
			Err: redactError(e.Err, token),
		}
	}
	original := err.Error()
	if message := redact.Replace(original); message != original {
		return &redactedError{message: message, cause: err}
	}
	return err
}
