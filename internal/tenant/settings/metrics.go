package settings

import (
	"context"
	"errors"

	"github.com/INKCR0W/yabot/internal/platform/store"
	"github.com/VictoriaMetrics/metrics"
)

const maxPendingPatchLogs = 256

type registryMetrics struct {
	patchOK       *metrics.Counter
	patchInvalid  *metrics.Counter
	patchConflict *metrics.Counter
	patchError    *metrics.Counter
	patchRejected *metrics.Counter
	patchLogWait  *metrics.Counter
	loadErrors    *metrics.Counter
	deleteBusy    *metrics.Counter
}

func newRegistryMetrics(set *metrics.Set) registryMetrics {
	if set == nil {
		return registryMetrics{}
	}
	return registryMetrics{
		patchOK:       set.GetOrCreateCounter(`yabot_settings_patch_total{outcome="ok"}`),
		patchInvalid:  set.GetOrCreateCounter(`yabot_settings_patch_total{outcome="invalid"}`),
		patchConflict: set.GetOrCreateCounter(`yabot_settings_patch_total{outcome="conflict"}`),
		patchError:    set.GetOrCreateCounter(`yabot_settings_patch_total{outcome="error"}`),
		patchRejected: set.GetOrCreateCounter(`yabot_settings_patch_total{outcome="rejected"}`),
		patchLogWait:  set.GetOrCreateCounter("yabot_settings_patch_log_wait_total"),
		loadErrors:    set.GetOrCreateCounter("yabot_settings_load_errors_total"),
		deleteBusy:    set.GetOrCreateCounter("yabot_settings_delete_user_busy_total"),
	}
}

func (r *Registry) reservePatchLog(ctx context.Context) error {
	if r.patchLogSlots == nil {
		return nil
	}
	// Reserve capacity before enqueueing so done can hand off log I/O without blocking the writer.
	select {
	case r.patchLogSlots <- struct{}{}:
		return nil
	default:
	}
	if r.metrics.patchLogWait != nil {
		r.metrics.patchLogWait.Inc()
	}
	select {
	case r.patchLogSlots <- struct{}{}:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (r *Registry) reportPatch(ctx context.Context, chatID int64, feature string, err error) {
	expected := r.countPatch(err)
	if r.log != nil && err != nil {
		r.logPatch(ctx, chatID, feature, err, expected)
	}
}

func (r *Registry) reportWrittenPatch(ctx context.Context, chatID int64, feature string, err error) {
	expected := r.countPatch(err)
	if r.patchLogSlots == nil {
		return
	}
	if err == nil {
		<-r.patchLogSlots
		return
	}
	go func() {
		defer func() { <-r.patchLogSlots }()
		r.logPatch(context.WithoutCancel(ctx), chatID, feature, err, expected)
	}()
}

func (r *Registry) countPatch(err error) (expected bool) {
	counter := r.metrics.patchError
	switch {
	case err == nil:
		counter = r.metrics.patchOK
	case errors.Is(err, ErrInvalid):
		counter, expected = r.metrics.patchInvalid, true
	case errors.Is(err, ErrConflict):
		counter, expected = r.metrics.patchConflict, true
	// A canceled request reaches the writer wrapped in *store.OpError and is still expected.
	case errors.Is(err, ErrUnknownFeature), errors.Is(err, store.ErrBusy),
		errors.Is(err, context.Canceled), errors.Is(err, context.DeadlineExceeded):
		counter, expected = r.metrics.patchRejected, true
	}
	if counter != nil {
		counter.Inc()
	}
	return expected
}

func (r *Registry) logPatch(ctx context.Context, chatID int64, feature string, err error, expected bool) {
	if expected {
		r.log.InfoContext(ctx, "settings patch rejected", "chat_id", chatID, "feature", feature, "error", err)
	} else {
		r.log.ErrorContext(ctx, "settings patch failed", "chat_id", chatID, "feature", feature, "error", err)
	}
}
