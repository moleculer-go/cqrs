package cqrs

import (
	"github.com/moleculer-go/store/sqlite"
)

// FieldsToSQLiteColumns create columns given the fields
// configuration return a list of sqlite columns.
func FieldsToSQLiteColumns(list ...map[string]interface{}) []sqlite.Column {
	uniqCols := map[string]sqlite.Column{}
	for _, fields := range list {
		for field, tp := range fields {
			str, isString := tp.(string)
			if isString {
				uniqCols[field] = sqlite.Column{field, str}
			}
		}
	}
	cols := make([]sqlite.Column, len(uniqCols))
	idx := 0
	for _, col := range uniqCols {
		cols[idx] = col
		idx = idx + 1
	}
	return cols
}
