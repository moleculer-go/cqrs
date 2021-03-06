package property

import (
	"github.com/moleculer-go/cqrs"
	"github.com/moleculer-go/moleculer"
	"github.com/moleculer-go/moleculer/payload"
	"github.com/moleculer-go/store"
	"github.com/moleculer-go/store/sqlite"
)

// storeFactory high order func that returns a cqrs.StoreFactory function :)
// and merges the fields passed to this function, with the fields received by the cqrs.StoreFactory func.
func storeFactory(fields ...map[string]interface{}) cqrs.StoreFactory {
	return func(name string, cqrsFields, settings map[string]interface{}) store.Adapter {
		fields = append(fields, cqrsFields)
		return &sqlite.Adapter{
			URI:     "file:memory:?mode=memory",
			Table:   name,
			Columns: cqrs.FieldsToSQLiteColumns(fields...),
		}
	}
}

var events = cqrs.EventStore("propertyEventStore", storeFactory())

var summaryAggregate = cqrs.Aggregate(
	"propertySummaryAggregate",
	storeFactory(map[string]interface{}{
		"countryCode": "string",
		"total":       "integer",
		"beachCity":   "integer",
		"mountain":    "integer",
	}),
	cqrs.NoSnapshot,
).Snapshot("propertyEventStore")

var propertiesAggregate = cqrs.Aggregate(
	"propertyAggregate",
	storeFactory(map[string]interface{}{
		"name":        "string",
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
	}),
	cqrs.NoSnapshot).Snapshot("propertyEventStore")

var Service = moleculer.ServiceSchema{
	Name:   "property",
	Mixins: []moleculer.Mixin{events.Mixin(), propertiesAggregate.Mixin(), summaryAggregate.Mixin()},
	Actions: []moleculer.Action{
		events.MapAction("create", "property.created"),
		{
			Name:    "transformProperty",
			Handler: transformProperty,
		},
		{
			Name:    "transformCountrySummary",
			Handler: transformCountrySummary,
		},
	},
	Events: []moleculer.Event{
		propertiesAggregate.On("property.created").Create("property.transformProperty"),
		summaryAggregate.On("property.created").Update("property.transformCountrySummary"),
	},
}

// transformProperty transform the property created event
// into then payload to be saved into the property aggregate
func transformProperty(context moleculer.Context, event moleculer.Payload) interface{} {
	return event.Get("payload")
}

// transformCountrySummary transform the property created event
// into a country summary update.
func transformCountrySummary(context moleculer.Context, event moleculer.Payload) interface{} {
	property := event.Get("payload")
	summary := <-context.Call("propertySummaryAggregate.find", map[string]interface{}{
		"countryCode": property.Get("countryCode").String(),
	})
	result := map[string]interface{}{}
	if summary.Len() > 0 {
		summary = summary.First()
		result["id"] = summary.Get("id").String()
	} else {
		summary = payload.New(map[string]interface{}{
			"countryCode": property.Get("countryCode").String(),
			"beachCity":   0,
			"mountain":    0,
			"total":       0,
		})
	}
	result["total"] = summary.Get("total").Int() + 1
	if isBeachCity(property) {
		result["beachCity"] = summary.Get("beachCity").Int() + 1
	}
	if isMountain(property) {
		result["mountain"] = summary.Get("mountain").Int() + 1
	}
	result["countryCode"] = summary.Get("countryCode").String()
	return result
}

func isBeachCity(property moleculer.Payload) bool {
	return property.Get("city").String() == "Tauranga"
}

func isMountain(property moleculer.Payload) bool {
	return property.Get("city").String() == "Wanaka"
}

// emitAll invoke all events.
func emitAll(eventHandlers ...moleculer.Event) moleculer.EventHandler {
	return func(context moleculer.Context, event moleculer.Payload) {
		for _, evtHandler := range eventHandlers {
			evtHandler.Handler(context, event)
		}
	}
}
