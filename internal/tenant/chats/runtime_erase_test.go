package chats

import (
	"context"
	"database/sql"
	"errors"
	"log/slog"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/INKCR0W/yabot/internal/platform/dispatch"
	"github.com/INKCR0W/yabot/internal/platform/store"
)

func TestRuntimeEraseReturnsWriteFailuresAndAllowsRetry(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name        string
		reason      string
		level       slog.Level
		message     string
		afterUpdate bool
	}{
		{"queue full", "busy", slog.LevelInfo, "chat user erasure is busy", false},
		{"operation rollback", "error", slog.LevelError, "chat user erasure failed", false},
		{"rollback after affected rows were counted", "error", slog.LevelError, "chat user erasure failed", true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			const chatID, userID = -100433, 81
			var reject atomic.Bool
			h := newRuntimeHarness(t, runtimeOptions{
				beforeStart: func(h *runtimeHarness) { h.seedActive(chatID, 1) },
				configure: func(d *Deps) {
					actual := d.Store
					d.Store = maintenanceStore{Store: actual, write: func(op store.Op, done func(error)) error {
						if reject.Swap(false) {
							if tc.afterUpdate {
								return actual.Write(func(tx *sql.Tx) error {
									if err := op(tx); err != nil {
										return err
									}
									return store.ErrRollbackOp
								}, done)
							}
							return store.ErrBusy
						}
						return actual.Write(op, done)
					}}
				},
			})
			if tc.reason == "busy" || tc.afterUpdate {
				reject.Store(true)
			} else {
				h.exec("CREATE TRIGGER reject_erasure BEFORE UPDATE ON chats WHEN NEW.owner_id IS NULL " +
					"BEGIN SELECT raise(ABORT, 'forced erasure rollback'); END")
			}
			ctx, cancel := context.WithTimeout(t.Context(), runtimeWait)
			defer cancel()
			err := h.chats.DeleteUser(ctx, userID)
			var opErr *store.OpError
			if tc.reason == "busy" && !errors.Is(err, store.ErrBusy) ||
				tc.reason == "error" && !errors.As(err, &opErr) {
				t.Fatalf("failed erasure returned %v", err)
			}
			h.assertLog(tc.level, tc.message)
			h.assertCounter("yabot_chats_erase_failures_total{reason=\""+tc.reason+"\"}", 1)
			if h.snapshot(chatID).OwnerID != 0 || storedChatIdentities(t, h, userID) != 1 {
				t.Fatal("failed erasure must clear memory and leave the SQLite row for retry")
			}
			if tc.reason == "error" && !tc.afterUpdate {
				h.exec("DROP TRIGGER reject_erasure")
			}
			if err := h.chats.DeleteUser(ctx, userID); err != nil {
				t.Fatalf("retry after erasure failure: %v", err)
			}
			assertStoredOwner(t, h, chatID, 0)
		})
	}
}

func TestRuntimeEraseCompletesWithNoShardTicketsAvailable(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name          string
		userID        int64
		snapshotOwner int64
		writes        int64
	}{
		{name: "missing user", snapshotOwner: 81},
		{name: "stored owner", userID: 81, writes: 2},
	} {
		t.Run(tc.name, func(t *testing.T) {
			const chatID = -100431
			var writes atomic.Int64
			h := newRuntimeHarness(t, runtimeOptions{
				tickets:     1,
				beforeStart: func(h *runtimeHarness) { h.seedActive(chatID, 1) },
				configure: func(d *Deps) {
					actual := d.Store
					d.Store = maintenanceStore{Store: actual, write: func(op store.Op, done func(error)) error {
						writes.Add(1)
						return actual.Write(op, done)
					}}
				},
			})
			tickets := make(chan dispatch.Ticket, 1)
			if err := h.dispatcher.Submit(keyOf(chatID), func(_ context.Context, sh *dispatch.Shard) {
				ticket, _ := sh.Admit()
				tickets <- ticket
			}); err != nil {
				t.Fatal(err)
			}
			ticket := runtimeReceive(t, tickets)
			defer ticket.Cancel()
			h.assertProbeTicket(1)
			ctx, cancel := context.WithTimeout(t.Context(), runtimeWait)
			defer cancel()
			if err := h.chats.DeleteUser(ctx, tc.userID); err != nil {
				t.Fatalf("erasure with all shard tickets in use: %v", err)
			}
			h.assertProbeTicket(1)
			if got := writes.Load(); got != tc.writes {
				t.Fatalf("erasure submitted %d writes, want %d", got, tc.writes)
			}
			if got := h.snapshot(chatID).OwnerID; got != tc.snapshotOwner {
				t.Fatalf("snapshot owner = %d, want %d", got, tc.snapshotOwner)
			}
			assertStoredOwner(t, h, chatID, tc.snapshotOwner)
		})
	}
}

func TestRuntimeEraseRetriesAnInFlightOwnerWriteAfterEnqueueFailure(t *testing.T) {
	t.Parallel()
	const chatID, previousOwner, claimant = -100444, 81, 95
	writing, allowCommit := make(chan error, 1), make(chan struct{})
	releaseWriter := sync.OnceFunc(func() { close(allowCommit) })
	retried := make(chan error, 1)
	var writes atomic.Int64
	h := newRuntimeHarness(t, runtimeOptions{
		beforeStart: func(h *runtimeHarness) { h.seedActive(chatID, 1) },
		configure: func(d *Deps) {
			actual := d.Store
			d.Store = maintenanceStore{Store: actual, write: func(op store.Op, done func(error)) error {
				switch writes.Add(1) {
				case 1:
					return actual.Write(func(tx *sql.Tx) error {
						err := op(tx)
						writing <- err
						<-allowCommit
						return err
					}, done)
				case 2:
					return store.ErrBusy
				case 3:
					err := actual.Write(op, done)
					retried <- err
					return err
				default:
					return actual.Write(op, done)
				}
			}}
		},
	})
	t.Cleanup(releaseWriter)
	token := claimToken(t, h, claimant, chatID)
	claimed := make(chan error, 1)
	go func() { claimed <- h.chats.SetOwner(t.Context(), chatID, claimant, token) }()
	if err := runtimeReceive(t, writing); err != nil {
		t.Fatalf("in-flight owner upsert: %v", err)
	}
	if err := h.chats.DeleteUser(t.Context(), claimant); !errors.Is(err, store.ErrBusy) {
		t.Fatalf("first erasure = %v, want queue-full error", err)
	}
	assertStoredOwner(t, h, chatID, previousOwner)
	if got := h.snapshot(chatID).OwnerID; got != previousOwner {
		t.Fatalf("pending claim visible before commit: %d", got)
	}

	deleted := make(chan error, 1)
	go func() { deleted <- h.chats.DeleteUser(t.Context(), claimant) }()
	select {
	case err := <-retried:
		if err != nil {
			t.Fatalf("enqueue retry: %v", err)
		}
	case err := <-deleted:
		t.Fatalf("retry returned %v before entering the writer", err)
	case <-time.After(runtimeWait):
		t.Fatal("retry never entered the writer")
	}
	releaseWriter()
	if err := runtimeReceive(t, claimed); !errors.Is(err, ErrBusy) {
		t.Fatalf("claim erased before commit returned %v, want ErrBusy", err)
	}
	if err := runtimeReceive(t, deleted); err != nil {
		t.Fatalf("retrying erasure: %v", err)
	}
	assertStoredOwner(t, h, chatID, 0)
	if got := h.snapshot(chatID).OwnerID; got != 0 {
		t.Fatalf("claim completion restored erased owner %d", got)
	}
}

func TestRuntimeEraseDuringClaimCompletion(t *testing.T) {
	t.Parallel()
	const chatID, previousOwner, claimant = -100441, 81, 95
	for _, tc := range []struct {
		name         string
		erase, owner int64
		claimErr     error
		rollBack     bool
	}{
		{"claimant", claimant, 0, ErrBusy, false},
		{"previous owner", previousOwner, claimant, nil, false},
		{"claimant after rollback", claimant, previousOwner, ErrBusy, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			committed, resume := make(chan error, 1), make(chan struct{})
			unblock := sync.OnceFunc(func() { close(resume) })
			var holdCompletion atomic.Bool
			h := newRuntimeHarness(t, runtimeOptions{
				beforeStart: func(h *runtimeHarness) { h.seedActive(chatID, 1) },
				configure: func(d *Deps) {
					actual := d.Store
					d.Store = maintenanceStore{Store: actual, write: func(op store.Op, done func(error)) error {
						if !holdCompletion.Swap(false) {
							return actual.Write(op, done)
						}
						if tc.rollBack {
							op = func(*sql.Tx) error { return store.ErrRollbackOp }
						}
						return actual.Write(op, func(err error) {
							go func() {
								committed <- err
								<-resume
								done(err)
							}()
						})
					}}
				},
			})
			t.Cleanup(unblock)
			token := claimToken(t, h, claimant, chatID)
			claimed := make(chan error, 1)
			holdCompletion.Store(true)
			go func() { claimed <- h.chats.SetOwner(t.Context(), chatID, claimant, token) }()
			err := runtimeReceive(t, committed)
			wantStored := int64(claimant)
			if tc.rollBack {
				var opErr *store.OpError
				if !errors.As(err, &opErr) {
					t.Fatalf("owner rollback: %v", err)
				}
				wantStored = previousOwner
			} else if err != nil {
				t.Fatalf("owner commit: %v", err)
			}
			assertStoredOwner(t, h, chatID, wantStored)
			if got := h.snapshot(chatID).OwnerID; got != previousOwner {
				t.Fatalf("claim published before its shard completion: owner = %d", got)
			}
			if err := h.chats.DeleteUser(t.Context(), tc.erase); err != nil {
				t.Fatal(err)
			}
			unblock()
			if err := runtimeReceive(t, claimed); !errors.Is(err, tc.claimErr) {
				t.Fatalf("claim completion returned %v, want %v", err, tc.claimErr)
			}
			if tc.claimErr == ErrBusy {
				h.assertClaimRejections(chatID, claimant, "erasure", 1)
			} else {
				h.assertClaimRejections(chatID, claimant, "erasure", 0)
			}
			if got := h.snapshot(chatID).OwnerID; got != tc.owner {
				t.Fatalf("completed claim published owner %d, want %d", got, tc.owner)
			}
			assertStoredOwner(t, h, chatID, tc.owner)
			h.barrier(chatID)
			h.assertProbeTicket(0)
		})
	}
}

func TestRuntimeEraseSkipsCanceledTargetShard(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name                 string
		detachDispatcher     bool
		cancelDispatcher     bool
		shardRunsAfterReturn bool
	}{
		{"canceled caller has returned", false, false, true},
		{"canceled caller still waiting for shard", true, false, false},
		{"dispatcher has stopped waiting for an accepted task", true, true, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			const chatID, userID = -100439, 81
			gate := newPrivacyRaceGate()
			settled := make(chan error, 1)
			dispatchCtx, cancelDispatch := context.WithCancel(t.Context())
			defer cancelDispatch()
			h := newRuntimeHarness(t, runtimeOptions{
				beforeStart: func(h *runtimeHarness) { h.seedActive(chatID, 1) },
				configure: func(d *Deps) {
					actual := d.Dispatcher
					d.Dispatcher = maintenanceDispatcher{Dispatcher: actual, runAll: func(ctx context.Context, task dispatch.AsyncTask) error {
						if !gate.armed.Load() {
							return actual.RunOnAll(ctx, task)
						}
						if tc.detachDispatcher {
							ctx = dispatchCtx
						}
						return actual.RunOnAll(ctx, func(shCtx context.Context, sh *dispatch.Shard, done func(error)) {
							gate.wait()
							task(shCtx, sh, func(err error) {
								done(err)
								settled <- err
							})
						})
					}}
				},
			})
			t.Cleanup(gate.release)
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			gate.armed.Store(true)
			result := make(chan error, 1)
			go func() { result <- h.chats.DeleteUser(ctx, userID) }()
			runtimeReceive(t, gate.reached)
			if tc.cancelDispatcher {
				cancelDispatch()
			} else {
				cancel()
			}
			awaitErasure := func() {
				t.Helper()
				if err := runtimeReceive(t, result); !errors.Is(err, context.Canceled) {
					t.Fatalf("erasure cancellation returned %v", err)
				}
			}
			if tc.shardRunsAfterReturn {
				awaitErasure()
			}
			gate.release()
			if err := runtimeReceive(t, settled); !errors.Is(err, context.Canceled) {
				t.Fatalf("late shard erasure settled with %v", err)
			}
			if !tc.shardRunsAfterReturn {
				awaitErasure()
			}
			h.barrier(chatID)
			if got := h.snapshot(chatID).OwnerID; got != userID {
				t.Fatalf("canceled target shard scrubbed owner %d", got)
			}
			assertStoredOwner(t, h, chatID, userID)
			claimToken(t, h, userID, chatID)
			if !tc.cancelDispatcher {
				h.assertCounter("yabot_chats_erase_failures_total{reason=\"error\"}", 0)
			}
		})
	}
}

func TestRuntimeEraseKeepsOverlappingErasuresActive(t *testing.T) {
	t.Parallel()
	const chatID, userID = -100437, 81
	first, second := newPrivacyRaceGate(), newPrivacyRaceGate()
	h := newRuntimeHarness(t, runtimeOptions{
		beforeStart: func(h *runtimeHarness) { h.seedActive(chatID, 1) },
		configure: func(d *Deps) {
			actual := d.Dispatcher
			d.Dispatcher = maintenanceDispatcher{Dispatcher: actual, runAll: func(ctx context.Context, task dispatch.AsyncTask) error {
				if !first.wait() {
					second.wait()
				}
				return actual.RunOnAll(ctx, task)
			}}
		},
	})
	t.Cleanup(first.release)
	t.Cleanup(second.release)
	firstResult, secondResult := make(chan error, 1), make(chan error, 1)
	first.armed.Store(true)
	go func() { firstResult <- h.chats.DeleteUser(t.Context(), userID) }()
	runtimeReceive(t, first.reached)
	second.armed.Store(true)
	go func() { secondResult <- h.chats.DeleteUser(t.Context(), userID) }()
	runtimeReceive(t, second.reached)
	first.release()
	if err := runtimeReceive(t, firstResult); err != nil {
		t.Fatal(err)
	}
	if _, err := h.chats.ClaimToken(userID, chatID); !errors.Is(err, ErrBusy) {
		t.Fatalf("token during second erasure = %v, want ErrBusy", err)
	}
	second.release()
	if err := runtimeReceive(t, secondResult); err != nil {
		t.Fatal(err)
	}
	claimToken(t, h, userID, chatID)
	assertStoredOwner(t, h, chatID, 0)
}

func TestRuntimeEraseBatchLimitAndSettlement(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name          string
		cancel        bool
		pausedBatch   int64
		rowsAtPause   int
		batchesAtExit int64
	}{
		{"canceled with the first batch accepted", true, 1, 1, 1},
		{"successful after a committed empty batch", false, 3, 0, 3},
	} {
		t.Run(tc.name, func(t *testing.T) {
			const first, last, userID = -200501, -200001, 84
			committed, release, callbackReturned := make(chan error, 1), make(chan struct{}), make(chan struct{})
			resume := sync.OnceFunc(func() { close(release) })
			var writes atomic.Int64
			h := newRuntimeHarness(t, runtimeOptions{
				tickets: 1,
				beforeStart: func(h *runtimeHarness) {
					h.exec("WITH RECURSIVE ids(id) AS (VALUES (?) UNION ALL SELECT id+1 FROM ids WHERE id < ?) "+
						"INSERT INTO chats (chat_id, type, state, updated_at, owner_id) "+
						"SELECT id, 'supergroup', 'active', ?, ? FROM ids", first, last, h.now().Unix(), userID)
				},
				configure: func(d *Deps) {
					actual := d.Store
					d.Store = maintenanceStore{Store: actual, write: func(op store.Op, done func(error)) error {
						if writes.Add(1) != tc.pausedBatch {
							return actual.Write(op, done)
						}
						return actual.Write(op, func(err error) {
							committed <- err
							<-release
							done(err)
							close(callbackReturned)
						})
					}}
				},
			})
			t.Cleanup(resume)
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			result := make(chan error, 1)
			go func() { result <- h.chats.DeleteUser(ctx, userID) }()
			if err := runtimeReceive(t, committed); err != nil {
				t.Fatalf("commit accepted erasure batch: %v", err)
			}
			if rows := storedChatIdentities(t, h, userID); rows != tc.rowsAtPause {
				t.Fatalf("committed batch left %d SQLite identities, want %d", rows, tc.rowsAtPause)
			}
			if tc.cancel {
				cancel()
				if err := runtimeReceive(t, result); !errors.Is(err, context.Canceled) {
					t.Fatalf("erasure cancellation returned %v", err)
				}
			} else {
				select {
				case err := <-result:
					t.Fatalf("erasure settled before empty batch completion: %v", err)
				default:
				}
			}
			h.assertProbeTicket(0)
			if _, err := h.chats.ClaimToken(userID, first); !errors.Is(err, ErrBusy) {
				t.Fatalf("claim during an accepted erasure batch returned %v, want ErrBusy", err)
			}
			resume()
			runtimeReceive(t, callbackReturned)
			h.barrier(first)
			if !tc.cancel {
				if err := runtimeReceive(t, result); err != nil {
					t.Fatal(err)
				}
			}
			h.assertProbeTicket(0)
			if got := writes.Load(); got != tc.batchesAtExit {
				t.Fatalf("submitted %d erasure batches, want %d", got, tc.batchesAtExit)
			}
			if tc.cancel {
				if err := h.chats.DeleteUser(t.Context(), userID); err != nil {
					t.Fatalf("retrying canceled erasure: %v", err)
				}
			}
			assertNoStoredChatIdentity(t, h, userID)
			claimToken(t, h, userID, first)
		})
	}
}
