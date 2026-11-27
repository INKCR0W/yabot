package settings

import (
	"context"
	"database/sql"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"testing/synctest"

	"github.com/INKCR0W/yabot/internal/platform/store"
	"github.com/INKCR0W/yabot/internal/tenant/contract"
)

func TestPurgeChatAbortsWhenValidityChangesInWriteQueue(t *testing.T) {
	watch, queued, completed := watchQueuedWrite(2)
	r, h, s := newSampleRegistry(t, watch)
	const chatID = 1
	initial := patchSample(t, h, chatID, `{"mode":"fast"}`, defaultETag)
	before := h.Get(chatID)
	resume := holdStoreWriter(t, s)

	var valid atomic.Bool
	valid.Store(true)
	returned := make(chan error, 1)
	go func() { returned <- r.PurgeChat(t.Context(), chatID, valid.Load) }()
	requireWriteQueued(t, queued, returned)
	valid.Store(false)
	resume()
	<-completed

	if err := <-returned; !errors.Is(err, contract.ErrAborted) {
		t.Errorf("PurgeChat = %v, want ErrAborted", err)
	}
	if got := countRows(t, s, `SELECT count(*) FROM chat_settings WHERE chat_id = ?`, chatID); got != 1 {
		t.Errorf("stored settings rows = %d, want 1", got)
	}
	if got := countRows(t, s, `SELECT count(*) FROM settings_audit WHERE chat_id = ?`, chatID); got != 1 {
		t.Errorf("audit rows = %d, want 1", got)
	}
	requireSampleSnapshot(t, r, h, chatID, before, initial.ETag)
}

func TestPatchCanceledInWriteQueueLeavesSettingsUnchanged(t *testing.T) {
	watch, queued, completed := watchQueuedWrite(2)
	r, h, s := newSampleRegistry(t, watch)
	const chatID = 1
	initial := patchSample(t, h, chatID, `{"mode":"fast"}`, defaultETag)
	before := h.Get(chatID)
	const auditQuery = `SELECT diff FROM settings_audit WHERE chat_id = ?`
	var auditBefore string
	if err := s.Read().QueryRowContext(t.Context(), auditQuery, chatID).Scan(&auditBefore); err != nil {
		t.Fatal(err)
	}
	resume := holdStoreWriter(t, s)

	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	patch := parsePatch(t, `{"enabled":true}`)
	returned := make(chan error, 1)
	go func() {
		_, err := h.Patch(ctx, chatID, Actor{}, patch, initial.ETag)
		returned <- err
	}()
	requireWriteQueued(t, queued, returned)
	cancel()
	resume()
	<-completed

	if err := <-returned; !errors.Is(err, context.Canceled) {
		t.Errorf("Patch = %v, want context.Canceled", err)
	}
	if got := countRows(t, s, `SELECT count(*) FROM chat_settings`); got != 1 {
		t.Errorf("stored settings rows = %d, want 1", got)
	}
	if got := storedSample(t, s, chatID); got != before {
		t.Errorf("stored settings = %+v, want %+v", got, before)
	}
	if got := storedETag(t, s, chatID); got != initial.ETag {
		t.Errorf("stored etag = %q, want %q", got, initial.ETag)
	}
	if got := countRows(t, s, `SELECT count(*) FROM settings_audit`); got != 1 {
		t.Errorf("audit rows = %d, want 1", got)
	}
	var auditAfter string
	if err := s.Read().QueryRowContext(t.Context(), auditQuery, chatID).Scan(&auditAfter); err != nil {
		t.Fatal(err)
	}
	if auditAfter != auditBefore {
		t.Errorf("audit diff = %q, want %q", auditAfter, auditBefore)
	}
	requireSampleSnapshot(t, r, h, chatID, before, initial.ETag)
}

func TestPatchCanceledAfterSQLPublishesCommittedSnapshot(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		pause, reached, resume := pauseWrite()
		defer resume()
		committed := make(chan error, 1)
		r, h, s := newSampleRegistry(t, interceptWrite(2, func(st Store, op store.Op, done func(error)) error {
			return st.Write(afterOp(op, pause), func(err error) {
				done(err)
				committed <- err
			})
		}))
		initial := patchSample(t, h, 1, `{"mode":"fast"}`, defaultETag)
		before := h.Get(1)
		ctx, cancel := context.WithCancel(t.Context())
		defer cancel()
		patch := parsePatch(t, `{"enabled":true}`)
		returned := make(chan error, 1)
		go func() {
			_, err := h.Patch(ctx, 1, Actor{}, patch, initial.ETag)
			returned <- err
		}()
		<-reached
		cancel()
		synctest.Wait()
		select {
		case err := <-returned:
			if !errors.Is(err, context.Canceled) {
				t.Fatalf("Patch = %v, want context.Canceled", err)
			}
		default:
			t.Fatal("Patch did not return after cancellation")
		}
		requireSampleSnapshot(t, r, h, 1, before, initial.ETag)

		resume()
		if err := <-committed; err != nil {
			t.Fatalf("commit = %v", err)
		}
		etag := storedETag(t, s, 1)
		if etag == initial.ETag || !storedSample(t, s, 1).Enabled {
			t.Fatal("the canceled request's update did not commit")
		}
		before.Enabled = true
		requireSampleSnapshot(t, r, h, 1, before, etag)
	})
}

func TestPatchCanCommitWhilePurgeChatIsWaitingToSubmit(t *testing.T) {
	for _, tc := range []struct {
		name      string
		patchChat int64
	}{
		{name: "another chat in the same snapshot shard", patchChat: 65},
		{name: "the chat being purged", patchChat: 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				pause, reached, resume := pauseWrite()
				defer resume()
				r, h, s := newSampleRegistry(t, interceptWrite(3, func(st Store, op store.Op, done func(error)) error {
					pause()
					return st.Write(op, done)
				}))
				initial := patchSample(t, h, 1, `{"mode":"fast"}`, defaultETag)
				other := patchSample(t, h, 65, `{"mode":"fast"}`, defaultETag)
				etag := initial.ETag
				if tc.patchChat == 65 {
					etag = other.ETag
				}
				purged := make(chan error, 1)
				go func() { purged <- r.PurgeChat(t.Context(), 1, func() bool { return true }) }()
				<-reached

				patch := parsePatch(t, `{"enabled":true}`)
				patched := make(chan error, 1)
				go func() {
					_, err := h.Patch(t.Context(), tc.patchChat, Actor{}, patch, etag)
					patched <- err
				}()
				synctest.Wait()
				select {
				case err := <-patched:
					if err != nil {
						t.Fatal(err)
					}
				default:
					t.Fatal("PurgeChat blocked Patch before submitting a delete")
				}
				if !storedSample(t, s, tc.patchChat).Enabled {
					t.Fatal("Patch did not commit before purge submission")
				}
				resume()
				if err := <-purged; err != nil {
					t.Fatal(err)
				}
				requirePurgedSample(t, r, h, s, 1)
				wantOther := sampleSettings{Enabled: tc.patchChat == 65, Mode: "fast", Count: 2, Delay: 300}
				if got := storedSample(t, s, 65); got != wantOther {
					t.Errorf("other chat in SQLite = %+v, want %+v", got, wantOther)
				}
				requireSampleSnapshot(t, r, h, 65, wantOther, storedETag(t, s, 65))
			})
		})
	}
}

func TestPatchWithOldETagConflictsIfPurgeCommitsFirst(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		pause, reached, resume := pauseWrite()
		defer resume()
		r, h, s := newSampleRegistry(t, interceptWrite(2, func(st Store, op store.Op, done func(error)) error {
			pause()
			return st.Write(op, done)
		}))
		initial := patchSample(t, h, 1, `{"mode":"fast"}`, defaultETag)
		patch := parsePatch(t, `{"enabled":true}`)
		patched := make(chan error, 1)
		go func() {
			_, err := h.Patch(t.Context(), 1, Actor{}, patch, initial.ETag)
			patched <- err
		}()
		<-reached
		purged := make(chan error, 1)
		go func() { purged <- r.PurgeChat(t.Context(), 1, func() bool { return true }) }()
		synctest.Wait()
		select {
		case err := <-purged:
			if err != nil {
				t.Fatal(err)
			}
		default:
			t.Fatal("an unsubmitted Patch blocked PurgeChat")
		}
		resume()
		if err := <-patched; !errors.Is(err, ErrConflict) {
			t.Fatalf("Patch with an erased etag = %v, want ErrConflict", err)
		}
		requirePurgedSample(t, r, h, s, 1)
	})
}

func TestPurgePublishesAtCommitBeforeNextPatch(t *testing.T) {
	pauseSQL, sqlFinished, commit := pauseWrite()
	pauseReturn, purgeCommitted, returnToPurge := pauseWrite()
	defer commit()
	defer returnToPurge()
	r, h, s := newSampleRegistry(t, interceptWrite(3, func(st Store, op store.Op, done func(error)) error {
		completed := make(chan error, 1)
		err := st.Write(afterOp(op, pauseSQL), func(err error) {
			done(err)
			completed <- err
		})
		if err == nil {
			<-completed
			pauseReturn()
		}
		return err
	}))
	first := patchSample(t, h, 1, `{"mode":"fast"}`, defaultETag)
	initial := patchSample(t, h, 1, `{"enabled":true}`, first.ETag)
	before := h.Get(1)
	purged := make(chan error, 1)
	go func() { purged <- r.PurgeChat(t.Context(), 1, func() bool { return true }) }()
	<-sqlFinished
	requireSampleSnapshot(t, r, h, 1, before, initial.ETag)
	commit()
	<-purgeCommitted
	requirePurgedSample(t, r, h, s, 1)

	newRow := patchSample(t, h, 1, `{"mode":"off"}`, defaultETag)
	returnToPurge()
	if err := <-purged; err != nil {
		t.Fatal(err)
	}
	want := sampleSettings{Mode: "off", Count: 2, Delay: 300}
	if got := storedSample(t, s, 1); got != want || storedETag(t, s, 1) != newRow.ETag {
		t.Errorf("new settings in SQLite = %+v, want %+v and etag %q", got, want, newRow.ETag)
	}
	requireSampleSnapshot(t, r, h, 1, want, newRow.ETag)
}

type interceptedWriteStore struct {
	Store
	writes atomic.Uint64
	hooks  map[uint64]writeHook
}

type writeHook func(Store, store.Op, func(error)) error

func interceptWrite(at uint64, run writeHook) func(*Deps) {
	return interceptWrites(map[uint64]writeHook{at: run})
}

func interceptWrites(hooks map[uint64]writeHook) func(*Deps) {
	return func(d *Deps) {
		d.Store = &interceptedWriteStore{Store: d.Store, hooks: hooks}
	}
}

func (s *interceptedWriteStore) Write(op store.Op, done func(error)) error {
	if run := s.hooks[s.writes.Add(1)]; run != nil {
		return run(s.Store, op, done)
	}
	return s.Store.Write(op, done)
}

func (s *interceptedWriteStore) WriteWait(ctx context.Context, op store.Op) error {
	done := make(chan error, 1)
	if err := s.Write(op, func(err error) { done <- err }); err != nil {
		return err
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	select {
	case err := <-done:
		return err
	case <-ctx.Done():
		return ctx.Err()
	}
}

func afterOp(op store.Op, pause func()) store.Op {
	return func(tx *sql.Tx) error {
		err := op(tx)
		pause()
		return err
	}
}

func pauseWrite() (pause func(), reached <-chan struct{}, resume func()) {
	started, release := make(chan struct{}), make(chan struct{})
	return func() {
		close(started)
		<-release
	}, started, sync.OnceFunc(func() { close(release) })
}

func watchQueuedWrite(at uint64) (setup func(*Deps), queued, completed <-chan struct{}) {
	accepted, finished := make(chan struct{}), make(chan struct{})
	setup = interceptWrite(at, func(st Store, op store.Op, done func(error)) error {
		err := st.Write(op, func(err error) {
			done(err)
			close(finished)
		})
		if err == nil {
			close(accepted)
		}
		return err
	})
	return setup, accepted, finished
}

func holdStoreWriter(t *testing.T, s *store.Store) func() {
	t.Helper()
	pause, reached, resume := pauseWrite()
	completed := make(chan error, 1)
	if err := s.Write(func(*sql.Tx) error {
		pause()
		return nil
	}, func(err error) { completed <- err }); err != nil {
		t.Fatalf("enqueue blocking write: %v", err)
	}
	select {
	case <-reached:
	case err := <-completed:
		t.Fatalf("blocking write finished before reaching its gate: %v", err)
	}
	t.Cleanup(func() {
		resume()
		if err := <-completed; err != nil {
			t.Errorf("blocking write: %v", err)
		}
	})
	return resume
}

func requireWriteQueued(t *testing.T, queued <-chan struct{}, returned <-chan error) {
	t.Helper()
	select {
	case <-queued:
	case err := <-returned:
		t.Fatalf("request returned before the write was queued: %v", err)
	}
}

func requireActorRows(t *testing.T, s *store.Store, actor int64, want int) {
	t.Helper()
	updated := countRows(t, s, `SELECT count(*) FROM chat_settings WHERE updated_by = ?`, actor)
	audit := countRows(t, s, `SELECT count(*) FROM settings_audit WHERE actor = ?`, actor)
	if updated != want || audit != want {
		t.Errorf("rows with identity: updated_by=%d, audit actor=%d, want %d each", updated, audit, want)
	}
}

func requireSampleSnapshot(t *testing.T, r *Registry, h *Handle[sampleSettings], chatID int64, want sampleSettings, etag string) {
	t.Helper()
	if got := h.Get(chatID); got != want {
		t.Errorf("Get(%d) = %+v, want %+v", chatID, got, want)
	}
	if got := r.Render(chatID, "en").Specs[0].ETag; got != etag {
		t.Errorf("Render(%d) etag = %q, want %q", chatID, got, etag)
	}
}

func storedETag(t *testing.T, s *store.Store, chatID int64) string {
	t.Helper()
	var etag string
	if err := s.Read().QueryRowContext(t.Context(), `SELECT etag FROM chat_settings WHERE chat_id = ?`, chatID).Scan(&etag); err != nil {
		t.Fatal(err)
	}
	return etag
}

func requirePurgedSample(t *testing.T, r *Registry, h *Handle[sampleSettings], s *store.Store, chatID int64) {
	t.Helper()
	if countRows(t, s, `SELECT count(*) FROM chat_settings WHERE chat_id = ?`, chatID) != 0 ||
		countRows(t, s, `SELECT count(*) FROM settings_audit WHERE chat_id = ?`, chatID) != 0 {
		t.Errorf("chat %d still has stored settings or an audit after purge", chatID)
	}
	requireSampleSnapshot(t, r, h, chatID, sampleSettings{Mode: "normal", Count: 2, Delay: 300}, defaultETag)
}
