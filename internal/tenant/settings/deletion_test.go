package settings

import (
	"context"
	"database/sql"
	"errors"
	"testing"
	"testing/synctest"

	"github.com/INKCR0W/yabot/internal/platform/store"
)

func TestDeleteUserAnonymizesPatchWaitingToSubmit(t *testing.T) {
	for _, tc := range []struct {
		name                string
		startDuringDeletion bool
	}{
		{name: "before deletion"},
		{name: "during deletion", startDuringDeletion: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				pausePatch, patchReachedStore, resumePatch := pauseWrite()
				defer resumePatch()
				pauseDelete, deleteReachedStore, resumeDelete := pauseWrite()
				defer resumeDelete()
				hooks := map[uint64]writeHook{2: beforeWrite(pausePatch)}
				if tc.startDuringDeletion {
					hooks[2], hooks[3] = beforeWrite(pauseDelete), beforeWrite(pausePatch)
				}
				r, h, s := newSampleRegistry(t, interceptWrites(hooks))
				actor := Actor{UserID: 42, Public: true}
				initial, err := h.Patch(t.Context(), 1, actor, parsePatch(t, `{"mode":"fast"}`), defaultETag)
				if err != nil {
					t.Fatal(err)
				}
				requireActorRows(t, s, actor.UserID, 1)

				var erased <-chan error
				if tc.startDuringDeletion {
					erased = startDeleteUser(t, r, actor.UserID)
					<-deleteReachedStore
				}
				patched := startPublicPatch(t, h, 1, actor.UserID, `{"enabled":true}`, initial.ETag)
				<-patchReachedStore
				if tc.startDuringDeletion {
					resumeDelete()
				} else {
					erased = startDeleteUser(t, r, actor.UserID)
				}
				requireFinished(t, "DeleteUser", erased)
				requireActorRows(t, s, actor.UserID, 0)

				resumePatch()
				requireFinished(t, "Patch", patched)
				requireActorRows(t, s, actor.UserID, 0)
				requireAnonymousUpdate(t, r, h, s, initial.ETag)
			})
		})
	}
}

func TestOverlappingDeleteUsersAnonymizePatchStartedBeforeSecondDeletion(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		pauseFirst, firstReachedStore, resumeFirst := pauseWrite()
		defer resumeFirst()
		pausePatch, patchReachedStore, resumePatch := pauseWrite()
		defer resumePatch()
		r, h, s := newSampleRegistry(t, interceptWrites(map[uint64]writeHook{
			1: beforeWrite(pauseFirst),
			2: beforeWrite(pausePatch),
		}))

		first := startDeleteUser(t, r, 42)
		<-firstReachedStore
		secondPatch := startPublicPatch(t, h, 1, 43, `{"enabled":true}`, defaultETag)
		<-patchReachedStore
		second := startDeleteUser(t, r, 43)
		requireFinished(t, "second DeleteUser", second)
		resumePatch()
		requireFinished(t, "second user's Patch", secondPatch)
		requireActorRows(t, s, 43, 0)
		requireAnonymousUpdate(t, r, h, s, defaultETag)
		resumeFirst()
		requireFinished(t, "first DeleteUser", first)
	})
}

func TestOverlappingDeletionsInOneBucketKeepAnonymizing(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		pauseDelete, deleteReachedStore, resumeDelete := pauseWrite()
		defer resumeDelete()
		pausePatch, patchReachedStore, resumePatch := pauseWrite()
		defer resumePatch()
		r, h, s := newSampleRegistry(t, interceptWrites(map[uint64]writeHook{
			1: beforeWrite(pauseDelete),
			3: beforeWrite(pausePatch),
		}))

		const userID = 42
		first := startDeleteUser(t, r, userID)
		<-deleteReachedStore
		second := startDeleteUser(t, r, userID)
		requireFinished(t, "second DeleteUser", second)

		patched := startPublicPatch(t, h, 1, userID, `{"enabled":true}`, defaultETag)
		<-patchReachedStore
		resumeDelete()
		requireFinished(t, "first DeleteUser", first)
		resumePatch()
		requireFinished(t, "Patch", patched)
		requireAnonymousUpdate(t, r, h, s, defaultETag)
	})
}

func TestConcurrentPatchesCanCommitWhileDeleteUserWaitsBetweenWrites(t *testing.T) {
	for _, tc := range []struct {
		name string
		rows int
	}{
		{name: "after terminal batch"},
		{name: "between batches", rows: 501},
	} {
		t.Run(tc.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				pause, batchCommitted, resume := pauseWrite()
				defer resume()
				r, h, s := newSampleRegistry(t, interceptWrite(1, func(st Store, op store.Op, done func(error)) error {
					completed := make(chan struct{})
					if err := st.Write(op, func(err error) {
						done(err)
						close(completed)
					}); err != nil {
						return err
					}
					<-completed
					pause()
					return nil
				}))
				if tc.rows > 0 {
					seedPrivacyAudit(t, s, 9, 42, 0, tc.rows)
				}
				erased := startDeleteUser(t, r, 42)
				<-batchCommitted

				other := startPublicPatch(t, h, 2, 43, `{"enabled":true}`, defaultETag)
				requireFinished(t, "other user's Patch", other)
				requireActorRows(t, s, 43, 1)

				same := startPublicPatch(t, h, 1, 42, `{"enabled":true}`, defaultETag)
				requireFinished(t, "erased user's Patch", same)
				requireAnonymousUpdate(t, r, h, s, defaultETag)
				resume()
				requireFinished(t, "DeleteUser", erased)
				requireActorRows(t, s, 42, 0)

				fresh := startPublicPatch(t, h, 1, 42, `{"mode":"fast"}`, storedETag(t, s, 1))
				requireFinished(t, "Patch started after DeleteUser", fresh)
				requireActorRows(t, s, 42, 1)
				requireActorRows(t, s, 43, 1)
			})
		})
	}
}

func TestDeleteUserClearsIdentityWrittenByAnOpAlreadyExecuting(t *testing.T) {
	pause, sqlFinished, resume := pauseWrite()
	defer resume()
	deleteQueued := make(chan struct{})
	r, h, s := newSampleRegistry(t, interceptWrites(map[uint64]writeHook{
		1: func(st Store, op store.Op, done func(error)) error {
			return st.Write(afterOp(op, pause), done)
		},
		2: func(st Store, op store.Op, done func(error)) error {
			err := st.Write(op, done)
			if err == nil {
				close(deleteQueued)
			}
			return err
		},
	}))
	t.Cleanup(resume)
	patched := startPublicPatch(t, h, 1, 42, `{"enabled":true}`, defaultETag)
	<-sqlFinished
	erased := startDeleteUser(t, r, 42)
	requireWriteQueued(t, deleteQueued, erased)
	resume()
	if err := <-patched; err != nil {
		t.Fatal(err)
	}
	if err := <-erased; err != nil {
		t.Fatal(err)
	}
	requireActorRows(t, s, 42, 0)
	requireAnonymousUpdate(t, r, h, s, defaultETag)
}

func TestFailedDeleteUserDoesNotLeaveBucketActive(t *testing.T) {
	r, h, s := newSampleRegistry(t, interceptWrite(1, func(Store, store.Op, func(error)) error {
		return store.ErrBusy
	}))
	const actor = 42
	if err := r.DeleteUser(t.Context(), actor); !errors.Is(err, store.ErrBusy) {
		t.Fatalf("DeleteUser = %v, want ErrBusy", err)
	}
	if _, err := h.Patch(t.Context(), 1, Actor{UserID: actor, Public: true}, parsePatch(t, `{"enabled":true}`), defaultETag); err != nil {
		t.Fatal(err)
	}
	requireActorRows(t, s, actor, 1)
}

func beforeWrite(pause func()) writeHook {
	return func(st Store, op store.Op, done func(error)) error {
		pause()
		return st.Write(op, done)
	}
}

func startDeleteUser(t *testing.T, r *Registry, userID int64) <-chan error {
	done := make(chan error, 1)
	go func(ctx context.Context) { done <- r.DeleteUser(ctx, userID) }(t.Context())
	return done
}

func startPublicPatch(t *testing.T, h *Handle[sampleSettings], chatID, userID int64, raw, etag string) <-chan error {
	t.Helper()
	patch := parsePatch(t, raw)
	done := make(chan error, 1)
	go func(ctx context.Context) {
		_, err := h.Patch(ctx, chatID, Actor{UserID: userID, Public: true}, patch, etag)
		done <- err
	}(t.Context())
	return done
}

func requireFinished(t *testing.T, operation string, done <-chan error) {
	t.Helper()
	synctest.Wait()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("%s returned %v", operation, err)
		}
	default:
		t.Fatalf("%s is still waiting", operation)
	}
}

func requireAnonymousUpdate(t *testing.T, r *Registry, h *Handle[sampleSettings], s *store.Store, previousETag string) {
	t.Helper()
	const chatID = 1
	var updatedBy, auditActor sql.NullInt64
	db := s.Read()
	if err := db.QueryRowContext(t.Context(), `SELECT updated_by FROM chat_settings WHERE chat_id = ?`, chatID).Scan(&updatedBy); err != nil {
		t.Fatal(err)
	}
	if err := db.QueryRowContext(t.Context(), `SELECT actor FROM settings_audit WHERE chat_id = ? ORDER BY id DESC LIMIT 1`, chatID).Scan(&auditActor); err != nil {
		t.Fatal(err)
	}
	if updatedBy.Valid || auditActor.Valid {
		t.Errorf("updated_by = %v, audit actor = %v, want NULL for both", updatedBy, auditActor)
	}
	etag, stored := storedETag(t, s, chatID), storedSample(t, s, chatID)
	if !stored.Enabled || etag == previousETag {
		t.Errorf("anonymous write left stale settings: %+v, etag %q", stored, etag)
	}
	requireSampleSnapshot(t, r, h, chatID, stored, etag)
}
