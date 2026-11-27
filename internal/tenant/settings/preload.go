package settings

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"unicode/utf8"
)

// Preload must finish during startup, before the registry serves requests.
func (r *Registry) Preload(ctx context.Context) (err error) {
	r.requireFrozen()
	rows, err := r.store.Read().QueryContext(ctx, `SELECT chat_id, feature, version, rev, data, etag FROM chat_settings`)
	if err != nil {
		return fmt.Errorf("query settings for preload: %w", err)
	}
	defer func() {
		if closeErr := rows.Close(); closeErr != nil {
			err = errors.Join(err, fmt.Errorf("close settings preload rows: %w", closeErr))
		}
	}()

	loaded := make([]snapshot, len(r.shards))
	for rows.Next() {
		var (
			key     rowKey
			row     entry
			version any
			raw     []byte
		)
		if err := rows.Scan(&key.chatID, &key.feature, &version, &row.rev, &raw, &row.etag); err != nil {
			r.reportLoadFailure(ctx, key.chatID, key.feature, "read row")
			continue
		}
		spec := r.features[key.feature]
		if spec == nil {
			if r.log != nil {
				r.log.InfoContext(ctx, "ignored settings for unregistered feature", "chat_id", key.chatID, "feature", key.feature)
			}
			continue
		}
		storedVersion, integer := version.(int64)
		if integer && int64(int(storedVersion)) == storedVersion {
			row.value = r.loadValue(ctx, key.chatID, spec, int(storedVersion), raw)
		} else {
			r.reportLoadFailure(ctx, key.chatID, key.feature, "invalid version")
			row.value = spec.defaults
		}
		shard := r.shardIndex(key.chatID)
		if loaded[shard] == nil {
			loaded[shard] = make(snapshot)
		}
		loaded[shard][key] = row
	}
	if err := rows.Err(); err != nil {
		return fmt.Errorf("read settings for preload: %w", err)
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	for i, rows := range loaded {
		r.shards[i].rows.Store(&rows)
	}
	return nil
}

func (r *Registry) loadValue(ctx context.Context, chatID int64, spec *registeredSpec, version int, raw json.RawMessage) any {
	if version > spec.Version {
		r.reportLoadFailure(ctx, chatID, spec.Feature, "unsupported version")
		return spec.defaults
	}
	if version < spec.Version {
		if spec.Migrate == nil {
			r.reportLoadFailure(ctx, chatID, spec.Feature, "missing migration")
			return spec.defaults
		}
		var err error
		raw, err = migrateValue(spec.Migrate, version, raw)
		if err != nil {
			reason := "migration failed"
			if errors.Is(err, errMigrationPanicked) {
				reason = "migration panicked"
			}
			r.reportLoadFailure(ctx, chatID, spec.Feature, reason)
			return spec.defaults
		}
	}
	value, err := spec.decodeStored(raw)
	if err != nil {
		r.reportLoadFailure(ctx, chatID, spec.Feature, "invalid stored values")
		return spec.defaults
	}
	return value
}

var errMigrationPanicked = errors.New("settings migration panicked")

func migrateValue(migrate func(int, json.RawMessage) (json.RawMessage, error), version int, raw json.RawMessage) (_ json.RawMessage, err error) {
	defer func() {
		if recover() != nil {
			err = errMigrationPanicked
		}
	}()
	return migrate(version, raw)
}

func (s *registeredSpec) decodeStored(raw json.RawMessage) (any, error) {
	if !utf8.Valid(raw) {
		return nil, errors.New("stored settings must be UTF-8")
	}
	var values map[string]json.RawMessage
	if err := json.Unmarshal(raw, &values); err != nil {
		return nil, err
	}
	if values == nil {
		return nil, errors.New("stored settings must be a JSON object")
	}
	value := reflect.New(s.typ).Elem()
	value.Set(reflect.ValueOf(s.defaults))
	for key, field := range s.fields {
		if raw, ok := values[key]; ok {
			decoded, err := field.decode(raw)
			if err != nil {
				return nil, err
			}
			value.Field(field.index).Set(decoded)
		}
	}
	return value.Interface(), nil
}

func (r *Registry) reportLoadFailure(ctx context.Context, chatID int64, feature, reason string) {
	if r.metrics.loadErrors != nil {
		r.metrics.loadErrors.Inc()
	}
	if r.log != nil {
		r.log.ErrorContext(ctx, "settings load failed", "chat_id", chatID, "feature", feature, "reason", reason)
	}
}
