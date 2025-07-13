package deps

import (
	"TFS/db"

	"gorm.io/gorm"
)

type Deps struct {
	DB   *gorm.DB
	Docs *db.DocumentStore
}
