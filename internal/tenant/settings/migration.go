package settings

import (
	_ "embed"

	"github.com/INKCR0W/yabot/internal/platform/store"
)

//go:embed schema.sql
var schemaSQL string

func Migrations() []store.Migration {
	return []store.Migration{{Component: "settings", Version: 1, SQL: schemaSQL}}
}
