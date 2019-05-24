package cqrs

import (
	"strings"

	"github.com/moleculer-go/moleculer"
	"github.com/moleculer-go/store"
	"github.com/moleculer-go/store/sqlite"
)

func createStoreService(name, extraFields, URI string) moleculer.ServiceSchema {
	return moleculer.ServiceSchema{
		Name: name,
		Mixins: []moleculer.Mixin{store.Mixin(&sqlite.Adapter{
			URI:     URI,
			Table:   name + "_table",
			Columns: storeColumns(extraFields),
		})},
		Started: func(c moleculer.BrokerContext, svc moleculer.ServiceSchema) {

		},
	}
}

// storeColumns return the columns for the event store table
func storeColumns(extraFields string) []sqlite.Column {
	defaultCols := []sqlite.Column{
		{
			Name: "event",
			Type: "string",
		},
		{
			Name: "created",
			Type: "integer",
		},
		{
			Name: "updated",
			Type: "integer",
		},
		{
			Name: "status",
			Type: "integer",
		},
		{
			Name: "payload",
			Type: "[]byte",
		},
	}
	if extraFields != "" {
		for _, f := range strings.Split(extraFields, ",") {
			defaultCols = append(defaultCols, sqlite.Column{
				Name: f,
				Type: "string",
			})
		}
	}
	return defaultCols
}
