package property

import (
	"errors"
	"fmt"

	"github.com/moleculer-go/cqrs"
	"github.com/moleculer-go/moleculer"
	"github.com/moleculer-go/moleculer/payload"
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

// sqliteStore used by the CQRS mixin to create
// the data store adapter for its models and backup on snapshots.
type sqliteStore struct {
	fields   map[string]interface{}
	settings map[string]interface{}
}

func (s *sqliteStore) Create(name string, cqrsFields, settings map[string]interface{}) store.Adapter {
	s.settings = settings
	return &sqlite.Adapter{
		URI:     resolveSQLiteURI(settings),
		Table:   name,
		Columns: cqrs.FieldsToSQLiteColumns(s.fields, cqrsFields),
	}
}

func (s *sqliteStore) Backup(name string) error {
	backupFolder, ok := s.settings["backupFolder"]
	if !ok {
		return errors.New("no backup folder setup! missing backupFolder from service settings! ")
	}
	return sqlite.FileCopyBackup(name, resolveSQLiteURI(s.settings), backupFolder.(string))
}

var events = cqrs.EventStore("propertyEventStore", &sqliteStore{})

var propertySummaryAg = cqrs.Aggregate("propertySummaryAggregate", &sqliteStore{fields: map[string]interface{}{
	"countryCode": "string",
	"total":       "integer",
	"beachCity":   "integer",
	"mountain":    "integer",
}}).Snapshot(events)

var propertiesAg = cqrs.Aggregate("propertyAggregate", &sqliteStore{fields: map[string]interface{}{
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
}}).Snapshot(events)

var Service = moleculer.ServiceSchema{
	Name:   "property",
	Mixins: []moleculer.Mixin{events.Mixin(), propertiesAg.Mixin(), propertySummaryAg.Mixin()},
	Actions: []moleculer.Action{
		{
			Name:    "create",
			Handler: events.PersistEvent("property.created"),
		},
	},
	Events: []moleculer.Event{
		{
			//property.created is fired by the persistent event store.
			Name: "property.created",
			Handler: emitAll(
				propertiesAg.Create(transformProperty),
				propertySummaryAg.Update(transformCountrySummary),
			),
		},
	},
}

// transformProperty transform the property created event
// into then payload to be saved into the property aggregate
func transformProperty(context moleculer.Context, event moleculer.Payload) moleculer.Payload {
	property := event.Get("payload")
	fmt.Println("transformProperty() property: ", property)
	return property
}

// transformCountrySummary transform the property created event
// into a country summary update.
func transformCountrySummary(context moleculer.Context, event moleculer.Payload) moleculer.Payload {
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
	return payload.New(result)
}

func isBeachCity(property moleculer.Payload) bool {
	return property.Get("city").String() == "Tauranga"
}

func isMountain(property moleculer.Payload) bool {
	return property.Get("city").String() == "Wanaka"
}

// emitAll invoke all events.
func emitAll(eventHandlers ...moleculer.EventHandler) moleculer.EventHandler {
	return func(context moleculer.Context, event moleculer.Payload) {
		for _, handler := range eventHandlers {
			handler(context, event)
		}
	}
}
