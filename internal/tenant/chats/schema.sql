CREATE TABLE chats (
    chat_id INTEGER PRIMARY KEY,
    type TEXT NOT NULL DEFAULT '',
    title TEXT NOT NULL DEFAULT '',
    state TEXT NOT NULL CHECK (state IN ('onboarding', 'active', 'degraded', 'removed')),
    bot_rights INTEGER NOT NULL DEFAULT 0,
    guard_assigned INTEGER NOT NULL DEFAULT 0,
    owner_id INTEGER,
    added_at INTEGER,
    updated_at INTEGER NOT NULL,
    ever_active INTEGER NOT NULL DEFAULT 0,
    removed_at INTEGER,
    removed_reason TEXT,
    migrated_to INTEGER,
    purge_after INTEGER,
    member_event_date INTEGER NOT NULL DEFAULT 0,
    member_event_update_id INTEGER NOT NULL DEFAULT 0
);

CREATE INDEX chats_owner_id ON chats (owner_id);
CREATE INDEX chats_state_purge_after ON chats (state, purge_after);
CREATE INDEX chats_state_updated_at ON chats (state, updated_at);
