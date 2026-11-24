package telegram

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/go-telegram/bot"
	"github.com/go-telegram/bot/models"
)

type Me struct {
	ID                         int64           `json:"id"`
	Username                   string          `json:"username"`
	SupportsJoinRequestQueries bool            `json:"supports_join_request_queries"`
	Raw                        json.RawMessage `json:"-"`
}

// GetMe requires a nonzero ID and returns the original result object in Raw. A missing capability field is false.
func (c *Client) GetMe(ctx context.Context) (Me, error) {
	raw, err := c.callJSON(ctx, "getMe", struct{}{})
	if err != nil {
		return Me{}, err
	}
	var me *Me
	if err := json.Unmarshal(raw, &me); err != nil {
		return Me{}, fmt.Errorf("decode telegram getMe result: %w", redactError(err, c.token))
	}
	if me == nil {
		return Me{}, errors.New("telegram getMe result must be a JSON object")
	}
	if me.ID == 0 {
		return Me{}, errors.New("telegram getMe result must have a nonzero id")
	}
	me.Raw = raw
	return *me, nil
}

// GetUpdates omits allowed_updates when allowed is nil. An undecodable update with a readable
// update_id is logged and replaced by an ID-only update. An unreadable ID rejects the batch.
func (c *Client) GetUpdates(ctx context.Context, offset int64, limit int, timeout time.Duration, allowed []string) ([]models.Update, error) {
	if limit < 1 || limit > 100 {
		return nil, errors.New("telegram getUpdates limit must be between 1 and 100")
	}
	params := struct {
		Offset         int64     `json:"offset"`
		Limit          int       `json:"limit"`
		Timeout        int64     `json:"timeout"`
		AllowedUpdates *[]string `json:"allowed_updates,omitempty"`
	}{Offset: offset, Limit: limit, Timeout: int64(max(0, timeout/time.Second))}
	if allowed != nil {
		params.AllowedUpdates = &allowed
	}
	result, err := c.callJSON(ctx, "getUpdates", params)
	if err != nil {
		return nil, err
	}
	raw, err := readUpdateBatch(result, limit)
	if err != nil {
		return nil, fmt.Errorf("decode telegram getUpdates result: %w", redactError(err, c.token))
	}
	updates := make([]models.Update, 0, len(raw))
	for _, entry := range raw {
		update, err := c.decodeUpdate(ctx, entry)
		if err != nil {
			return nil, err
		}
		updates = append(updates, update)
	}
	return updates, nil
}

func readUpdateBatch(result json.RawMessage, limit int) ([]json.RawMessage, error) {
	decoder := json.NewDecoder(bytes.NewReader(result))
	start, err := decoder.Token()
	if err != nil {
		return nil, err
	}
	if start == nil {
		return nil, nil
	}
	if start != json.Delim('[') {
		return nil, errors.New("telegram getUpdates result must be an array")
	}
	// The byte cap alone cannot bound the number of decoded elements.
	batch := make([]json.RawMessage, 0, limit)
	for decoder.More() {
		if len(batch) == limit {
			return nil, fmt.Errorf("telegram getUpdates response exceeds requested limit %d", limit)
		}
		var entry json.RawMessage
		if err := decoder.Decode(&entry); err != nil {
			return nil, err
		}
		batch = append(batch, entry)
	}
	if _, err := decoder.Token(); err != nil {
		return nil, err
	}
	return batch, nil
}

func (c *Client) decodeUpdate(ctx context.Context, raw json.RawMessage) (models.Update, error) {
	var update models.Update
	decodeErr := json.Unmarshal(raw, &update)
	if decodeErr == nil && update.ID != 0 {
		return update, nil
	}
	var header struct {
		ID *int64 `json:"update_id"`
	}
	if err := json.Unmarshal(raw, &header); err != nil {
		return models.Update{}, fmt.Errorf("decode telegram update_id: %w", redactError(err, c.token))
	}
	if header.ID == nil {
		return models.Update{}, errors.New("telegram update has no update_id")
	}
	if decodeErr == nil {
		return update, nil
	}
	var typeErr *json.UnmarshalTypeError
	if !errors.As(decodeErr, &typeErr) {
		// Some model unmarshallers include fragments of the input in their errors.
		decodeErr = fmt.Errorf("telegram update model rejected its payload (%T)", decodeErr)
	}
	c.log.ErrorContext(ctx, "decode telegram update", "update_id", *header.ID, "error", redactError(decodeErr, c.token))
	return models.Update{ID: *header.ID}, nil
}

type apiResponse struct {
	OK          bool            `json:"ok"`
	Result      json.RawMessage `json:"result"`
	ErrorCode   int             `json:"error_code"`
	Description string          `json:"description"`
	Parameters  struct {
		RetryAfter int `json:"retry_after"`
		MigrateTo  int `json:"migrate_to_chat_id"`
	} `json:"parameters"`
}

func (c *Client) callJSON(ctx context.Context, method string, payload any) (json.RawMessage, error) {
	body, err := json.Marshal(payload)
	if err != nil {
		return nil, fmt.Errorf("encode telegram %s request: %w", method, redactError(err, c.token))
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.serverURL+"/bot"+c.token+"/"+method, bytes.NewReader(body))
	if err != nil {
		return nil, fmt.Errorf("create telegram %s request: %w", method, redactError(err, c.token))
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := c.http.Do(req)
	if err != nil {
		return nil, fmt.Errorf("send telegram %s request: %w", method, redactError(err, c.token))
	}
	body, err = readBody(resp)
	if err != nil {
		return nil, fmt.Errorf("read telegram %s response: %w", method, withHTTPStatusError(method, resp, err))
	}
	envelope, err := c.decodeAPIResponse(method, resp, body)
	if err != nil {
		return nil, err
	}
	if !envelope.OK {
		return nil, envelope.apiError(method)
	}
	return envelope.Result, nil
}

func (c *Client) decodeAPIResponse(method string, resp *http.Response, body []byte) (apiResponse, error) {
	var envelope apiResponse
	decodeErr := json.Unmarshal(body, &envelope)
	status := resp.StatusCode
	if status >= 400 && status < 600 && (decodeErr != nil || envelope.OK || envelope.ErrorCode == 0) {
		return apiResponse{}, httpStatusError(method, resp)
	}
	if decodeErr != nil {
		return apiResponse{}, fmt.Errorf("decode telegram %s response: %w", method, redactError(decodeErr, c.token))
	}
	if !envelope.OK {
		envelope.Description = tokenReplacer(c.token).Replace(envelope.Description)
	}
	return envelope, nil
}

func readBody(resp *http.Response) (body []byte, err error) {
	defer func() {
		if closeErr := resp.Body.Close(); closeErr != nil {
			body = nil
			err = errors.Join(err, closeErr)
		}
	}()
	body, err = io.ReadAll(limitResponseBody(resp))
	if err != nil {
		return nil, err
	}
	return body, nil
}

// Match upstream raw_request.go, where migration is a special case of HTTP 400.
func (r apiResponse) apiError(method string) error {
	switch r.ErrorCode {
	case http.StatusBadRequest:
		if r.Parameters.MigrateTo != 0 {
			return &bot.MigrateError{
				Message:         fmt.Sprintf("%s: %s", bot.ErrorBadRequest, r.Description),
				MigrateToChatID: r.Parameters.MigrateTo,
			}
		}
		return fmt.Errorf("%w, %s", bot.ErrorBadRequest, r.Description)
	case http.StatusUnauthorized:
		return fmt.Errorf("%w, %s", bot.ErrorUnauthorized, r.Description)
	case http.StatusForbidden:
		return fmt.Errorf("%w, %s", bot.ErrorForbidden, r.Description)
	case http.StatusNotFound:
		return fmt.Errorf("%w, %s", bot.ErrorNotFound, r.Description)
	case http.StatusConflict:
		return fmt.Errorf("%w, %s", bot.ErrorConflict, r.Description)
	case http.StatusTooManyRequests:
		return &bot.TooManyRequestsError{
			Message:    fmt.Sprintf("%s, %s", bot.ErrorTooManyRequests, r.Description),
			RetryAfter: r.Parameters.RetryAfter,
		}
	default:
		return ordinaryAPIError(method, r.ErrorCode, r.Description)
	}
}

func ordinaryAPIError(method string, code int, description string) error {
	return fmt.Errorf("%s%s, %d %s", apiErrorPrefix, method, code, description)
}

func httpStatusError(method string, resp *http.Response) error {
	status := resp.StatusCode
	fallback := apiResponse{ErrorCode: status, Description: http.StatusText(status)}
	if status == http.StatusTooManyRequests {
		fallback.Parameters.RetryAfter = retryAfterSeconds(resp.Header.Get("Retry-After"), time.Now())
	}
	return fallback.apiError(method)
}

func withHTTPStatusError(method string, resp *http.Response, err error) error {
	if resp.StatusCode >= 200 && resp.StatusCode < 300 {
		return err
	}
	return errors.Join(httpStatusError(method, resp), err)
}

func retryAfterSeconds(value string, now time.Time) int {
	const maxSeconds = min(1<<(strconv.IntSize-1)-1, (1<<63-1)/uint64(time.Second))
	value = strings.TrimSpace(value)
	if seconds, err := strconv.ParseUint(value, 10, 64); err == nil ||
		(errors.Is(err, strconv.ErrRange) && strings.Trim(value, "0123456789") == "") {
		return int(min(seconds, maxSeconds))
	}
	retryAt, err := http.ParseTime(value)
	if err != nil {
		return 1
	}
	delay := max(0, retryAt.Sub(now))
	seconds := uint64(delay / time.Second)
	if delay%time.Second != 0 {
		seconds++
	}
	return int(min(seconds, maxSeconds))
}
