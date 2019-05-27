package property

import (
	"github.com/moleculer-go/cqrs"
	"github.com/moleculer-go/moleculer"
	"github.com/moleculer-go/store"
	"github.com/moleculer-go/store/sqlite"
)

// resolveSQLiteURI check if is memory of file based db.
func resolveSQLiteURI(settings map[string]interface{}) string {
	folder, ok := settings["sqliteFolder"]
	if !ok || folder.(string) == "memory" {
		return "file:memory:?mode=memory"
	}
	return "file://" + folder.(string)
}

// adapterFactory used by the CQRS mixin to create adapter for its models.
func adapterFactory(fields map[string]interface{}) cqrs.AdapterFactory {
	return func(name string, cqrsFields, settings map[string]interface{}) store.Adapter {
		return &sqlite.Adapter{
			URI:     resolveSQLiteURI(settings),
			Table:   name,
			Columns: cqrs.FieldsToSQLiteColumns(fields, cqrsFields),
		}
	}
}

var property = cqrs.Aggregate("property", adapterFactory(map[string]interface{}{
	"name": "string",
	//"headline":"string", or title ?
	"title":       "string",
	"description": "string",
	"active":      "bool",
	"owner":       "string",
	"bedrooms":    "integer",
	"bathrooms":   "float",
	"maxGuests":   "integer",
	"sqrMeters":   "integer",
	//The primary street address of the property.
	"addressOne": "string",
	//The secondary street address of the property.
	"addressTwo":  "string",
	"city":        "string",
	"region":      "string",
	"postalCode":  "string",
	"countryCode": "string",
	"latitude":    "float",
	"longitude":   "float",
	"source": map[string]string{
		"name":      "string",
		"sourceId":  "string",
		"sourceIds": "map",
	},
}))

var events = cqrs.EventStore("property", adapterFactory(map[string]interface{}{}))

var service = moleculer.ServiceSchema{
	Name:   "property",
	Mixins: []moleculer.Mixin{events.Mixin(), property.Mixin()},
	Actions: []moleculer.Action{
		{
			Name:    "create",
			Handler: events.NewEvent("property.created"),
		},
	},
	Events: []moleculer.Event{
		{
			//property.created is fired by the persistent event store.
			Name:    "property.created",
			Handler: propertyAggregate.Create(transformProperty),
		},
	},
}

// transformProperty transform the property created event
// into then payload to be saved into the aggregate
func transformProperty(context moleculer.Context, event moleculer.Payload) moleculer.Payload {

	//save record on aggregate

	//context.Emit("property.created.successfully", property)
}
