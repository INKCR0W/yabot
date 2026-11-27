package initdata_test

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"maps"
	"net/url"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/INKCR0W/yabot/internal/platform/miniapp/initdata"
)

const (
	testToken       = "test-only-signing-token"
	testAuthSeconds = 1_780_000_000
	testMaxAge      = 24 * time.Hour
)

func TestVerifyIdentity(t *testing.T) {
	now := time.Unix(testAuthSeconds, 0)
	for _, test := range []struct {
		name  string
		extra url.Values
		want  initdata.Identity
	}{
		{
			name: "guard launch",
			extra: url.Values{
				"user":                       {`{"id":4503599627370495,"language_code":"zh_TW","first_name":"Example"}`},
				"chat":                       {`{"id":-4503599627370493,"type":"supergroup"}`},
				"chat_join_request_query_id": {"join_query"},
				"signature":                  {"opaque-signature"},
			},
			want: initdata.Identity{UserID: 4503599627370495, Lang: "zh_TW", ChatID: -4503599627370493, JoinQueryID: "join_query", AuthDate: now},
		},
		{
			name:  "direct link",
			extra: url.Values{"start_param": {"c-123"}},
			want:  initdata.Identity{UserID: 42, StartParam: "c-123", AuthDate: now},
		},
		{
			name: "optional fields omitted",
			want: initdata.Identity{UserID: 42, AuthDate: now},
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			form := sampleForm()
			maps.Copy(form, test.extra)
			got, err := initdata.Verify(signedForm(form), testToken, now, testMaxAge)
			if err != nil || got != test.want {
				t.Fatalf("Verify() = %+v, %v, want %+v", got, err, test.want)
			}
		})
	}
}

func TestVerifyRejectsTamperedFields(t *testing.T) {
	form := sampleForm()
	form.Set("chat", `{"id":-123}`)
	form.Set("chat_join_request_query_id", "join_query")
	form.Set("query_id", "query")
	form.Set("signature", "opaque-signature")
	form.Set("start_param", "start")
	hash := signLines(testToken, checkLines(form))
	now := time.Unix(testAuthSeconds, 0)
	for _, edit := range []struct {
		name  string
		apply func(url.Values, string)
	}{
		{"change", func(form url.Values, key string) { form.Set(key, "tampered value") }},
		{"delete", func(form url.Values, key string) { form.Del(key) }},
	} {
		for _, key := range slices.Sorted(maps.Keys(form)) {
			t.Run(edit.name+"/"+key, func(t *testing.T) {
				tampered := maps.Clone(form)
				edit.apply(tampered, key)
				requireVerifyError(t, withHash(tampered, hash), testToken, now, testMaxAge, initdata.ErrBadHash)
			})
		}
	}
	t.Run("add field", func(t *testing.T) {
		tampered := maps.Clone(form)
		tampered.Set("extra", "added value")
		requireVerifyError(t, withHash(tampered, hash), testToken, now, testMaxAge, initdata.ErrBadHash)
	})
	t.Run("change one hash digit", func(t *testing.T) {
		digit := "0"
		if hash[0] == '0' {
			digit = "1"
		}
		requireVerifyError(t, withHash(form, digit+hash[1:]), testToken, now, testMaxAge, initdata.ErrBadHash)
	})
}

func TestVerifyBotToken(t *testing.T) {
	const otherToken = "another-test-signing-token"
	form := sampleForm()
	lines := checkLines(form)
	signed := withHash(form, signLines(testToken, lines))
	otherSigned := withHash(form, signLines(otherToken, lines))
	now := time.Unix(testAuthSeconds, 0)
	for _, test := range []struct {
		name, raw, token string
		want             error
	}{
		{"first token matches", signed, testToken, nil},
		{"second token matches", otherSigned, otherToken, nil},
		{"first token cannot verify second signature", otherSigned, testToken, initdata.ErrBadHash},
		{"second token cannot verify first signature", signed, otherToken, initdata.ErrBadHash},
	} {
		t.Run(test.name, func(t *testing.T) {
			requireVerifyError(t, test.raw, test.token, now, testMaxAge, test.want)
		})
	}
}

func TestVerifyFormEncoding(t *testing.T) {
	const joinQueryID = "plus+ percent% space & =\r\x00中文 😀"
	form := sampleForm()
	form.Set("chat_join_request_query_id", joinQueryID)
	form.Set("CHAT_JOIN_REQUEST_QUERY_ID", "separate field")
	form.Set("start_param", "x=y;z")
	form.Set("bare", "")
	form.Set("é", "non-ASCII key")
	form.Set("key\r\x00;", "opaque key")
	signed := signedForm(form)
	for _, test := range []struct {
		name string
		raw  string
	}{
		{"spaces as plus and literal plus as percent escape", signed},
		{"spaces as percent escapes", strings.ReplaceAll(signed, "+", "%20")},
		{"bare field and unescaped value punctuation", "&" + strings.NewReplacer("bare=", "bare", "x%3Dy%3Bz", "x=y;z").Replace(signed) + "&&"},
	} {
		t.Run(test.name, func(t *testing.T) {
			now := time.Unix(testAuthSeconds, 0)
			got, err := initdata.Verify(test.raw, testToken, now, testMaxAge)
			want := initdata.Identity{UserID: 42, AuthDate: now, JoinQueryID: joinQueryID, StartParam: "x=y;z"}
			if err != nil || got != want {
				t.Fatalf("Verify() = %+v, %v, want %+v", got, err, want)
			}
		})
	}
	t.Run("line feed in value", func(t *testing.T) {
		form.Set("chat_join_request_query_id", "plus+ percent% space & =\n中文 😀")
		requireVerifyError(t, signedForm(form), testToken, time.Unix(testAuthSeconds, 0), testMaxAge, initdata.ErrMalformed)
	})
}

func TestVerifyRejectsAmbiguousFieldBoundaries(t *testing.T) {
	form := sampleForm()
	form.Set("signature", "opaque-signature")
	form.Set("start_param", "launch=details")
	hash := signLines(testToken, checkLines(form))
	now := time.Unix(testAuthSeconds, 0)
	requireVerifyError(t, withHash(form, hash), testToken, now, testMaxAge, nil)

	for _, test := range []struct {
		name, remove, key, value string
	}{
		{"merge start param into signature", "start_param", "signature", form.Get("signature") + "\nstart_param=" + form.Get("start_param")},
		{"merge user into start param", "user", "start_param", form.Get("start_param") + "\nuser=" + form.Get("user")},
		{"equals in key", "start_param", "start_param=launch", "details"},
	} {
		t.Run(test.name, func(t *testing.T) {
			form := maps.Clone(form)
			form.Del(test.remove)
			form.Set(test.key, test.value)
			requireVerifyError(t, withHash(form, hash), testToken, now, testMaxAge, initdata.ErrMalformed)
		})
	}
}

func TestVerifySigningRules(t *testing.T) {
	prefixFields := sampleForm()
	prefixFields.Set("a", "first")
	prefixFields.Set("a2", "second")
	wrongOrder := checkLines(prefixFields)
	slices.Sort(wrongOrder)

	reversedPairs := strings.Split(signedForm(prefixFields), "&")
	slices.Reverse(reversedPairs)

	signatureFields := sampleForm()
	signatureFields.Set("signature", "opaque-signature")
	for _, test := range []struct {
		name string
		raw  string
		want error
	}{
		{"prefix keys sorted", signedForm(prefixFields), nil},
		{"whole lines sorted", withHash(prefixFields, signLines(testToken, wrongOrder)), initdata.ErrBadHash},
		{"request order reversed", strings.Join(reversedPairs, "&"), nil},
		{"signature included", signedForm(signatureFields), nil},
		{"signature omitted from signing", withHash(signatureFields, signLines(testToken, checkLines(sampleForm()))), initdata.ErrBadHash},
	} {
		t.Run(test.name, func(t *testing.T) {
			requireVerifyError(t, test.raw, testToken, time.Unix(testAuthSeconds, 0), testMaxAge, test.want)
		})
	}
}

func TestVerifyAuthDate(t *testing.T) {
	now := time.Unix(testAuthSeconds, 0)
	for _, test := range []struct {
		name, authDate string
		now            time.Time
		want           error
	}{
		{"age at limit", strconv.FormatInt(testAuthSeconds-86400, 10), now, nil},
		{"past limit by one second", strconv.FormatInt(testAuthSeconds-86401, 10), now, initdata.ErrExpired},
		{"past limit by one nanosecond", strconv.FormatInt(testAuthSeconds-86400, 10), now.Add(time.Nanosecond), initdata.ErrExpired},
		{"future within tolerance", strconv.FormatInt(testAuthSeconds+119, 10), now, nil},
		{"future at tolerance", strconv.FormatInt(testAuthSeconds+120, 10), now, nil},
		{"future past tolerance by one second", strconv.FormatInt(testAuthSeconds+121, 10), now, initdata.ErrFuture},
		{"future past tolerance by one nanosecond", strconv.FormatInt(testAuthSeconds+120, 10), now.Add(-time.Nanosecond), initdata.ErrFuture},
		{"largest int64 timestamp in future", "9223372036854775807", now, initdata.ErrFuture},
		{"missing", "", now, initdata.ErrMalformed},
		{"non-numeric", "private timestamp", now, initdata.ErrMalformed},
		{"fractional", "1780000000.5", now, initdata.ErrMalformed},
		{"zero", "0", now, initdata.ErrMalformed},
		{"negative", "-1", now, initdata.ErrMalformed},
		{"integer overflow", "9223372036854775808", now, initdata.ErrMalformed},
	} {
		t.Run(test.name, func(t *testing.T) {
			form := sampleForm()
			if test.authDate == "" {
				form.Del("auth_date")
			} else {
				form.Set("auth_date", test.authDate)
			}
			requireVerifyError(t, signedForm(form), testToken, test.now, testMaxAge, test.want)
		})
	}
}

func TestVerifyMaxAge(t *testing.T) {
	signed := signedForm(sampleForm())
	authDate := time.Unix(testAuthSeconds, 0)
	twoHoursLater := authDate.Add(2 * time.Hour)
	twentyFiveHoursLater := authDate.Add(25 * time.Hour)
	const shortMaxAge = 30 * time.Second
	for _, test := range []struct {
		name   string
		now    time.Time
		maxAge time.Duration
		want   error
	}{
		{"two hours old with one hour limit", twoHoursLater, time.Hour, initdata.ErrExpired},
		{"two hours old with three hour limit", twoHoursLater, 3 * time.Hour, nil},
		{"older than one day within limit", twentyFiveHoursLater, 26 * time.Hour, nil},
		{"short limit at boundary", authDate.Add(shortMaxAge), shortMaxAge, nil},
		{"short limit exceeded by one second", authDate.Add(shortMaxAge + time.Second), shortMaxAge, initdata.ErrExpired},
	} {
		t.Run(test.name, func(t *testing.T) {
			requireVerifyError(t, signed, testToken, test.now, test.maxAge, test.want)
		})
	}
}

func TestVerifyHashFormat(t *testing.T) {
	form := sampleForm()
	hash := signLines(testToken, checkLines(form))
	for _, test := range []struct {
		name, raw string
		want      error
	}{
		{"missing", form.Encode(), initdata.ErrMissingHash},
		{"empty", withHash(form, ""), initdata.ErrBadHash},
		{"non-hex", withHash(form, "z"+hash[1:]), initdata.ErrBadHash},
		{"too short", withHash(form, hash[:62]), initdata.ErrBadHash},
		{"too long", withHash(form, hash+"00"), initdata.ErrBadHash},
		{"uppercase hex", withHash(form, strings.ToUpper(hash)), nil},
	} {
		t.Run(test.name, func(t *testing.T) {
			requireVerifyError(t, test.raw, testToken, time.Unix(testAuthSeconds, 0), testMaxAge, test.want)
		})
	}
}

func TestVerifyMalformedForm(t *testing.T) {
	form := sampleForm()
	form.Set("a", "same")
	signed := signedForm(form)
	for _, test := range []struct{ name, suffix string }{
		{"duplicate key", "&user=duplicate"},
		{"duplicate encoded key", "&%75ser=duplicate"},
		{"duplicate encoded key with identical value", "&%61=same"},
		{"duplicate hash", "&hash=duplicate"},
		{"empty key", "&=private"},
		{"line feed in key", "&extra%0Akey=private"},
		{"invalid escape in key", "&%ZZ=private"},
		{"invalid escape in value", "&extra=private%"},
	} {
		t.Run(test.name, func(t *testing.T) {
			requireVerifyError(t, signed+test.suffix, testToken, time.Unix(testAuthSeconds, 0), testMaxAge, initdata.ErrMalformed)
		})
	}
}

func TestVerifyUserAndChat(t *testing.T) {
	for _, test := range []struct {
		name, field, value string
		remove             bool
		want               error
	}{
		{"missing user", "user", "", true, initdata.ErrNoUser},
		{"missing user id", "user", `{"first_name":"Private"}`, false, initdata.ErrNoUser},
		{"zero user id", "user", `{"id":0}`, false, initdata.ErrNoUser},
		{"negative user id", "user", `{"id":-1}`, false, initdata.ErrNoUser},
		{"empty user", "user", "", false, initdata.ErrMalformed},
		{"malformed user", "user", `{"id":1,"first_name":"Private"`, false, initdata.ErrMalformed},
		{"wrong user id type", "user", `{"id":"42"}`, false, initdata.ErrMalformed},
		{"empty chat", "chat", "", false, initdata.ErrMalformed},
		{"malformed chat", "chat", `{"id":-123,"title":"Private"`, false, initdata.ErrMalformed},
		{"wrong chat id type", "chat", `{"id":1.5}`, false, initdata.ErrMalformed},
	} {
		t.Run(test.name, func(t *testing.T) {
			form := sampleForm()
			if test.remove {
				form.Del(test.field)
			} else {
				form.Set(test.field, test.value)
			}
			requireVerifyError(t, signedForm(form), testToken, time.Unix(testAuthSeconds, 0), testMaxAge, test.want)
		})
	}
}

func TestVerifyValidationOrder(t *testing.T) {
	now := time.Unix(testAuthSeconds, 0)
	noDate := sampleForm()
	noDate.Del("auth_date")
	noDate.Del("user")
	expired := sampleForm()
	expired.Set("auth_date", strconv.FormatInt(now.Add(-testMaxAge-time.Second).Unix(), 10))
	expiredWithoutUser := maps.Clone(expired)
	expiredWithoutUser.Del("user")
	futureWithMalformedUser := sampleForm()
	futureWithMalformedUser.Set("auth_date", strconv.FormatInt(now.Add(2*time.Minute+time.Second).Unix(), 10))
	futureWithMalformedUser.Set("user", "invalid json")
	noUser := sampleForm()
	noUser.Del("user")
	noUser.Set("chat", "invalid json")
	freshHash := signLines(testToken, checkLines(sampleForm()))
	for _, test := range []struct {
		name, raw, token string
		maxAge           time.Duration
		want             error
	}{
		{"empty token before parsing", "malformed=%", "", testMaxAge, initdata.ErrInvalidArgument},
		{"zero max age before parsing", "malformed=%", testToken, 0, initdata.ErrInvalidArgument},
		{"negative max age before parsing", "malformed=%", testToken, -time.Second, initdata.ErrInvalidArgument},
		{"missing hash before date or user", noDate.Encode(), testToken, testMaxAge, initdata.ErrMissingHash},
		{"bad hash before date or user", withHash(noDate, freshHash), testToken, testMaxAge, initdata.ErrBadHash},
		{"bad hash before expiration", withHash(expired, freshHash), testToken, testMaxAge, initdata.ErrBadHash},
		{"missing date before missing user", signedForm(noDate), testToken, testMaxAge, initdata.ErrMalformed},
		{"expiration before missing user", signedForm(expiredWithoutUser), testToken, testMaxAge, initdata.ErrExpired},
		{"future date before malformed user", signedForm(futureWithMalformedUser), testToken, testMaxAge, initdata.ErrFuture},
		{"user before chat", signedForm(noUser), testToken, testMaxAge, initdata.ErrNoUser},
	} {
		t.Run(test.name, func(t *testing.T) {
			requireVerifyError(t, test.raw, test.token, now, test.maxAge, test.want)
		})
	}
}

func TestVerifyErrorRedactsFieldValues(t *testing.T) {
	const (
		privateDate  = "private-timestamp"
		privateUser  = "private-user-name"
		privateChat  = "private-chat-title"
		privateStart = "private-start-param"
		privateQuery = "private-query-id"
		privateJoin  = "private-join-id"
		privateSig   = "private-signature"
	)
	form := sampleForm()
	form.Set("user", `{"id":42,"first_name":"`+privateUser+`"}`)
	form.Set("chat", `{"id":-123,"title":"`+privateChat+`"}`)
	form.Set("start_param", privateStart)
	form.Set("query_id", privateQuery)
	form.Set("chat_join_request_query_id", privateJoin)
	form.Set("signature", privateSig)
	now := time.Unix(testAuthSeconds, 0)
	for _, test := range []struct{ name, field, value string }{
		{"malformed auth date", "auth_date", privateDate},
		{"malformed user JSON", "user", strings.TrimSuffix(form.Get("user"), "}")},
		{"malformed chat JSON", "chat", strings.TrimSuffix(form.Get("chat"), "}")},
	} {
		t.Run(test.name, func(t *testing.T) {
			form := maps.Clone(form)
			form.Set(test.field, test.value)
			hash := signLines(testToken, checkLines(form))
			requireVerifyError(t, withHash(form, hash), testToken, now, testMaxAge, initdata.ErrMalformed,
				form.Get("auth_date"), privateUser, privateChat, privateStart, privateQuery, privateJoin, privateSig, hash)
		})
	}
}

func requireVerifyError(t *testing.T, raw, token string, now time.Time, maxAge time.Duration, want error, privateValues ...string) {
	t.Helper()
	got, err := initdata.Verify(raw, token, now, maxAge)
	if !errors.Is(err, want) {
		t.Fatalf("Verify() error = %v, want %v", err, want)
	}
	if err != nil {
		if got != (initdata.Identity{}) {
			t.Errorf("Verify() returned an identity on error: %+v", got)
		}
		for _, secret := range append([]string{raw, token}, privateValues...) {
			if secret != "" && strings.Contains(err.Error(), secret) {
				t.Error("Verify() error contains private input")
			}
		}
	}
}

func sampleForm() url.Values {
	return url.Values{
		"auth_date": {strconv.FormatInt(testAuthSeconds, 10)},
		"user":      {`{"id":42}`},
	}
}

func signedForm(form url.Values) string {
	return withHash(form, signLines(testToken, checkLines(form)))
}

func checkLines(form url.Values) []string {
	var lines []string
	for _, name := range slices.Sorted(maps.Keys(form)) {
		lines = append(lines, fmt.Sprintf("%s=%s", name, form.Get(name)))
	}
	return lines
}

func signLines(token string, lines []string) string {
	deriveKey := hmac.New(sha256.New, []byte("WebAppData"))
	deriveKey.Write([]byte(token))
	check := hmac.New(sha256.New, deriveKey.Sum(nil))
	check.Write([]byte(strings.Join(lines, "\n")))
	return hex.EncodeToString(check.Sum(nil))
}

func withHash(form url.Values, hash string) string {
	return form.Encode() + "&hash=" + url.QueryEscape(hash)
}
