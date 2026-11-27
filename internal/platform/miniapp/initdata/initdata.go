package initdata

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"net/url"
	"slices"
	"strconv"
	"strings"
	"time"
)

type verifyError string

func (e verifyError) Error() string { return string(e) }

const (
	ErrMalformed       verifyError = "initdata: malformed data"
	ErrMissingHash     verifyError = "initdata: missing hash"
	ErrBadHash         verifyError = "initdata: invalid hash"
	ErrExpired         verifyError = "initdata: expired data"
	ErrFuture          verifyError = "initdata: auth date is in the future"
	ErrNoUser          verifyError = "initdata: missing user identity"
	ErrInvalidArgument verifyError = "initdata: invalid verification argument"
)

type Identity struct {
	UserID      int64
	Lang        string
	AuthDate    time.Time
	ChatID      int64
	JoinQueryID string
	StartParam  string
}

// Verify checks Telegram's bot-token signature before interpreting identity fields.
// maxAge must be positive, and auth dates up to two minutes ahead of now are accepted.
func Verify(raw, botToken string, now time.Time, maxAge time.Duration) (Identity, error) {
	if botToken == "" || maxAge <= 0 {
		return Identity{}, ErrInvalidArgument
	}
	fields, err := parseForm(raw)
	if err != nil {
		return Identity{}, err
	}
	hashText, ok := fields["hash"]
	if !ok {
		return Identity{}, ErrMissingHash
	}
	if len(hashText) != hex.EncodedLen(sha256.Size) {
		return Identity{}, ErrBadHash
	}
	var providedHash [sha256.Size]byte
	if _, err := hex.Decode(providedHash[:], []byte(hashText)); err != nil {
		return Identity{}, ErrBadHash
	}
	delete(fields, "hash")
	secret := sign([]byte("WebAppData"), botToken)
	if !hmac.Equal(providedHash[:], sign(secret, dataCheckString(fields))) {
		return Identity{}, ErrBadHash
	}

	const futureTolerance = 2 * time.Minute
	authSeconds, err := strconv.ParseInt(fields["auth_date"], 10, 64)
	if err != nil || authSeconds <= 0 {
		return Identity{}, ErrMalformed
	}
	earliest := now.Add(-maxAge)
	if authSeconds < earliest.Unix() || authSeconds == earliest.Unix() && earliest.Nanosecond() != 0 {
		return Identity{}, ErrExpired
	}
	if authSeconds > now.Add(futureTolerance).Unix() {
		return Identity{}, ErrFuture
	}

	var user struct {
		ID   int64  `json:"id"`
		Lang string `json:"language_code"`
	}
	userJSON, ok := fields["user"]
	if !ok {
		return Identity{}, ErrNoUser
	}
	if err := json.Unmarshal([]byte(userJSON), &user); err != nil {
		return Identity{}, ErrMalformed
	}
	if user.ID <= 0 {
		return Identity{}, ErrNoUser
	}
	var chat struct {
		ID int64 `json:"id"`
	}
	if chatJSON, ok := fields["chat"]; ok {
		if err := json.Unmarshal([]byte(chatJSON), &chat); err != nil {
			return Identity{}, ErrMalformed
		}
	}
	return Identity{
		UserID:      user.ID,
		Lang:        user.Lang,
		AuthDate:    time.Unix(authSeconds, 0),
		ChatID:      chat.ID,
		JoinQueryID: fields["chat_join_request_query_id"],
		StartParam:  fields["start_param"],
	}, nil
}

func parseForm(raw string) (map[string]string, error) {
	fields := make(map[string]string)
	for pair := range strings.SplitSeq(raw, "&") {
		if pair == "" {
			continue
		}
		rawKey, rawValue, _ := strings.Cut(pair, "=")
		// Preserve the field boundaries of the signed key=value lines.
		key, err := url.QueryUnescape(rawKey)
		if err != nil || key == "" || strings.ContainsRune(key, '\n') || strings.ContainsRune(key, '=') {
			return nil, ErrMalformed
		}
		if _, exists := fields[key]; exists {
			return nil, ErrMalformed
		}
		value, err := url.QueryUnescape(rawValue)
		if err != nil || strings.ContainsRune(value, '\n') {
			return nil, ErrMalformed
		}
		fields[key] = value
	}
	return fields, nil
}

func dataCheckString(fields map[string]string) string {
	keys := make([]string, 0, len(fields))
	for key := range fields {
		keys = append(keys, key)
	}
	slices.Sort(keys)
	var check strings.Builder
	for i, key := range keys {
		if i > 0 {
			check.WriteByte('\n')
		}
		check.WriteString(key)
		check.WriteByte('=')
		check.WriteString(fields[key])
	}
	return check.String()
}

func sign(key []byte, message string) []byte {
	mac := hmac.New(sha256.New, key)
	mac.Write([]byte(message))
	return mac.Sum(nil)
}
