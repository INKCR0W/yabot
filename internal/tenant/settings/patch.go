package settings

import (
	"context"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"maps"
	"math"
	"reflect"
	"strings"

	"github.com/INKCR0W/yabot/internal/platform/store"
)

const (
	invalidValueKey = "settings.error.invalid_value"
	unknownKeyKey   = "settings.error.unknown_key"
)

func (r *Registry) Patch(ctx context.Context, feature string, chatID int64, actor Actor, patch map[string]json.RawMessage, ifMatch string) (_ PatchResult, err error) {
	r.requireFrozen()
	submitted := false
	defer func() {
		if !submitted {
			r.reportPatch(ctx, chatID, feature, err)
		}
	}()
	if err := ctx.Err(); err != nil {
		return PatchResult{}, err
	}
	spec := r.features[feature]
	if spec == nil {
		return PatchResult{}, ErrUnknownFeature
	}
	var bucket *deletionBucket
	var deletionState uint64
	if actor.Public {
		bucket = r.deletionBucket(actor.UserID)
		deletionState = bucket.state.Load()
	}
	current := r.current(spec, chatID)
	value, fields, err := spec.applyPatch(current.value, patch)
	if err := ctx.Err(); err != nil {
		return PatchResult{}, err
	}
	if err != nil {
		return PatchResult{Fields: fields}, err
	}
	if strings.TrimSpace(ifMatch) != current.etag {
		return PatchResult{}, ErrConflict
	}
	values := spec.values(value)
	if value == current.value {
		return PatchResult{ETag: current.etag, Values: values, Fields: fields}, nil
	}

	next, err := r.nextEntry(current, value)
	if err != nil {
		return PatchResult{}, err
	}
	data, diff, err := encodeChange(spec, current.value, values)
	if err != nil {
		return PatchResult{}, err
	}
	key := rowKey{chatID, feature}
	updatedAt := r.now().Unix()
	submitted, err = r.writePatch(ctx, key, next, func(tx *sql.Tx) error {
		if err := ctx.Err(); err != nil {
			return err
		}
		var actorID any
		if bucket != nil && bucket.permitsIdentity(deletionState) {
			actorID = actor.UserID
		}
		return persistPatch(tx, key, spec.Version, current, next, data, diff, actorID, updatedAt)
	})
	if err != nil {
		return PatchResult{}, err
	}
	return PatchResult{ETag: next.etag, Values: values, Fields: fields}, nil
}

func (r *Registry) writePatch(ctx context.Context, key rowKey, next entry, op store.Op) (bool, error) {
	if err := r.reservePatchLog(ctx); err != nil {
		return false, err
	}
	done := make(chan error, 1)
	err := r.store.Write(op, func(err error) {
		if err == nil {
			r.publish(key, next)
		}
		r.reportWrittenPatch(ctx, key.chatID, key.feature, err)
		done <- err
	})
	if err != nil {
		if r.patchLogSlots != nil {
			<-r.patchLogSlots
		}
		return false, fmt.Errorf("submit settings patch: %w", err)
	}
	select {
	case <-ctx.Done():
		return true, ctx.Err()
	case err := <-done:
		if canceled := ctx.Err(); canceled != nil {
			return true, canceled
		}
		if err != nil {
			if errors.Is(err, ErrConflict) {
				return true, ErrConflict
			}
			return true, fmt.Errorf("commit settings patch: %w", err)
		}
		return true, nil
	}
}

func (s *registeredSpec) applyPatch(current any, patch map[string]json.RawMessage) (any, map[string]FieldResult, error) {
	previous := reflect.ValueOf(current)
	candidate := reflect.New(s.typ).Elem()
	candidate.Set(previous)
	fields := make(map[string]FieldResult, len(patch))
	for key, raw := range patch {
		field := s.fields[key]
		if field == nil {
			fields[key] = FieldResult{Code: CodeUnknownKey, Key: unknownKeyKey}
			continue
		}
		value, err := field.decode(raw)
		if err != nil {
			fields[key] = FieldResult{Code: CodeInvalidValue, Key: invalidValueKey}
			continue
		}
		candidate.Field(field.index).Set(value)
		fields[key] = FieldResult{Code: CodeOK}
	}
	merged := reflect.New(s.typ).Elem()
	merged.Set(previous)
	for key := range patch {
		field := s.fields[key]
		if field == nil {
			continue
		}
		if !field.visible(candidate) {
			fields[key] = FieldResult{Code: CodeOK}
			continue
		}
		if fields[key].Code == CodeOK {
			merged.Field(field.index).Set(candidate.Field(field.index))
		}
	}
	if s.Validate != nil {
		input := reflect.New(s.typ)
		input.Elem().Set(merged)
		fieldErrors, err := validatePatch(s.Validate, input.Interface())
		if err != nil {
			return nil, nil, err
		}
		for _, fieldError := range fieldErrors {
			message := fieldError.Message
			if message == "" {
				message = invalidValueKey
			}
			fields[fieldError.Key] = FieldResult{Code: CodeInvalidValue, Key: message, Args: maps.Clone(fieldError.Args)}
		}
	}
	for _, result := range fields {
		if result.Code != CodeOK {
			return nil, fields, ErrInvalid
		}
	}
	return merged.Interface(), fields, nil
}

func validatePatch(validate func(any) []FieldError, value any) (fields []FieldError, err error) {
	defer func() {
		if recover() != nil {
			err = errors.New("settings validation panicked")
		}
	}()
	return validate(value), nil
}

func (r *Registry) nextEntry(current entry, value any) (entry, error) {
	if current.rev == math.MaxInt64 {
		return entry{}, errors.New("settings revision limit exhausted")
	}
	var random [16]byte
	if _, err := io.ReadFull(r.random, random[:]); err != nil {
		return entry{}, fmt.Errorf("generate settings etag: %w", err)
	}
	etag := `"` + hex.EncodeToString(random[:]) + `"`
	if etag == current.etag {
		return entry{}, errors.New("generated settings etag matches the current etag")
	}
	return entry{value: value, rev: current.rev + 1, etag: etag}, nil
}

func persistPatch(tx *sql.Tx, key rowKey, version int, current, next entry, data, diff []byte, actorID any, updatedAt int64) error {
	var result sql.Result
	var err error
	if current.etag == defaultETag {
		result, err = tx.Exec(`INSERT INTO chat_settings (chat_id, feature, version, rev, data, etag, updated_by, updated_at)
			VALUES (?, ?, ?, ?, ?, ?, ?, ?) ON CONFLICT (chat_id, feature) DO NOTHING`,
			key.chatID, key.feature, version, next.rev, string(data), next.etag, actorID, updatedAt)
	} else {
		result, err = tx.Exec(`UPDATE chat_settings SET version = ?, rev = ?, data = ?, etag = ?, updated_by = ?, updated_at = ?
			WHERE chat_id = ? AND feature = ? AND etag = ?`,
			version, next.rev, string(data), next.etag, actorID, updatedAt, key.chatID, key.feature, current.etag)
	}
	if err != nil {
		return fmt.Errorf("write settings row: %w", err)
	}
	rows, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("count updated settings rows: %w", err)
	}
	if rows == 0 {
		return ErrConflict
	}
	if _, err := tx.Exec(`INSERT INTO settings_audit (chat_id, feature, actor, diff, created_at)
		VALUES (?, ?, ?, ?, ?)`, key.chatID, key.feature, actorID, string(diff), updatedAt); err != nil {
		return fmt.Errorf("append settings audit: %w", err)
	}
	return nil
}
