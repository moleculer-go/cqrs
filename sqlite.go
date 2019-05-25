package cqrs

import (
	"github.com/moleculer-go/store/sqlite"
)

//FieldsToSQLiteColumns createColumns given the fields configuration return a list oif sqlite columns.
func FieldsToSQLiteColumns(list ...map[string]interface{}) []sqlite.Column {
	cols := []sqlite.Column{}
	for _, fields := range list {
		for field, tp := range fields {
			str, isString := tp.(string)
			if isString {
				cols = append(cols, sqlite.Column{field, str})
			}
		}
	}
	return cols
}
