CREATE TABLE chat_settings (
    chat_id INTEGER NOT NULL,
    feature TEXT NOT NULL,
    version INTEGER NOT NULL,
    rev INTEGER NOT NULL,
    data JSON NOT NULL,
    etag TEXT NOT NULL,
    updated_by INTEGER,
    updated_at INTEGER NOT NULL,
    PRIMARY KEY (chat_id, feature)
);

CREATE INDEX chat_settings_updated_by ON chat_settings (updated_by);

CREATE TABLE settings_audit (
    id INTEGER PRIMARY KEY,
    chat_id INTEGER NOT NULL,
    feature TEXT NOT NULL,
    actor INTEGER,
    diff JSON NOT NULL,
    created_at INTEGER NOT NULL
);

CREATE INDEX settings_audit_chat_id ON settings_audit (chat_id);
CREATE INDEX settings_audit_actor ON settings_audit (actor);
CREATE INDEX settings_audit_created_at ON settings_audit (created_at);
