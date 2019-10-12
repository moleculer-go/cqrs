# CQRS Implementation using Moleculer-Go

CQRS Mixins

Make you life easy when doing CQRS back-ends :)

## Imports
```go
import (
	"github.com/moleculer-go/cqrs"
    "github.com/moleculer-go/store"
    // if you are using sqlite as the store mechanism
	"github.com/moleculer-go/store/sqlite"
)
```

```go
//event store
var events = cqrs.EventStore("propertyEventStore", storeFactory())

// create an aggregate (simple table to store computed values)
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

//service that expose the actions and perform the transformation
var Service = moleculer.ServiceSchema{
	Name:   "property",
	Mixins: []moleculer.Mixin{events.Mixin(), propertiesAggregate.Mixin(), summaryAggregate.Mixin()},
	Actions: []moleculer.Action{
        {
			Name:    "create",
			Handler: events.PersistEvent("property.created"),
		},
		{
			Name:    "transformCountrySummary",
			Handler: transformCountrySummary,
		},
	},
	Events: []moleculer.Event{
		summaryAggregate.On("property.created").Update("property.transformCountrySummary"),
	},
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

// ...
// using it

//create a property
property := <-bkr.Call("property.create", M{
			"name":        "Beach Villa",
			"active":      true,
			"bathrooms":   1.5,
			"city":        "Wanaka",
			"countryCode": "NZ",
		})

// propertySummaryAggregate now contains a record for coutnry code NZ and mountain = 1 and total = 1

// see more in the examples folder.

```


## Event Store

```go
// this creates a event store called propertyEventStore
// storeFactory() is displayed bellow
var events = cqrs.EventStore("propertyEventStore", storeFactory())

// ...
// storeFactory high order func that returns a cqrs.StoreFactory function :)
// and merges the fields passed to this function, with the fields received by the cqrs.StoreFactory func.
func storeFactory(fields ...map[string]interface{}) cqrs.StoreFactory {
	return func(name string, cqrsFields, settings map[string]interface{}) store.Adapter {
        fields = append(fields, cqrsFields)
        //creates an store adapter, in this case SQLite
        //the store adapter is use to store data of the eventStore, so here is where
        //you define the back end of your eventStore. 
        // You could use mongo.MongoAdapter to have your eventStore backed by mongo DB.
		return &sqlite.Adapter{
			URI:     "file:memory:?mode=memory",
			Table:   name,
			Columns: cqrs.FieldsToSQLiteColumns(fields...),
		}
	}
}

```

### Storing Events
```go
// service property has an action create that will store the event "property.created" with the payload
// sent to the action.
var Service = moleculer.ServiceSchema{
	Name:   "property",
	Mixins: []moleculer.Mixin{events.Mixin(), propertiesAggregate.Mixin(), summaryAggregate.Mixin()},
	Actions: []moleculer.Action{
        {
            Name:    "create",

            // events.PersistEvent() creates a moleculer Action handle that will respond to the action.
            // when the action property.create is invoked the eventStore will save the event an respond back with an standard answer.
            Handler: events.PersistEvent("property.created"),
        },
        //...

```

### Pumping and handling events
```go
//same service as above, now is to demonstrate how you handle events dispatched by the event pump.
var Service = moleculer.ServiceSchema{
	Name:   "property",
	Mixins: []moleculer.Mixin{events.Mixin(), propertiesAggregate.Mixin(), summaryAggregate.Mixin()},
	Actions: []moleculer.Action{
        //these are transformation actions that are invoked by the aggregate handlers.
       {
            Name:    "transformProperty",
            Handler: transformProperty,
        },
        {
			Name:    "transformCountrySummary",
			Handler: transformCountrySummary,
		},
    Events: []moleculer.Event{
            // the event pump will dispatch a "property.created" event for every "property.created" event stored.
            //It does it at different times, the save process only saves, then the event pump dispatch the events.
            propertiesAggregate.On("property.created").Create("property.transformProperty"),
            summaryAggregate.On("property.created").Update("property.transformCountrySummary"),
        },
        //...
```

## Aggregates

Aggregates are database tables. Simple as that. They store the "calculated values" or the "state of the system" :)
Basically there are never queries on events. Events are dispatched and any moleculer service can listen to the them.
Aggregates have an api to map event -> transformation -> aggregate action
```go
 propertiesAggregate.On("property.created").Create("property.transformProperty"),
 ```
 The code above is listening to event "property.created", it will use the transformation action "property.transformProperty" and it will invoke the aggregate action create.
 ```go
 summaryAggregate.On("property.created").Update("property.transformCountrySummary"),
 ```
  The code above is listening to event "property.created", it will use the transformation action "property.transformCountrySummary" and it will invoke the aggregate action Update.

  ### Define your Aggregates
  Aggregate are tables of data and depending on the data store they need an schema.
  In this case with SQLite we need to specify the fields in our aggregate.
  ```go
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
  ```
  An aggregate contain all the actions available to any store (https://github.com/moleculer-go/store)
  So you can just do:
  ```go
  summaries := <-bkr.Call("propertySummaryAggregate.find", M{})
  ```


## Snapshots

Snapshots are a work in progress. basic is implemented and now under test.

snapshotName := aggregate.snapshot():
 - aggregate stops listening to events -> pause aggregate changes :)
 - create a snapshot event -> new events will continue to be recorded in the events store after this point! = Write is enabled.
 - ** no changes are happening on aggregates ** but reads continue happily.
 - backup aggregates -> aggregate.backup(snapshotName)  (SQLLite -> basicaly copy files :) )
 - process all events since the snapshot and start listening to events again.
 - done.
 - Error scenarios:
 - if backup fails the event is marked as failed and is ignored when trying to restore events.
 - Rationale:
 ---> Since you created an event about the start of the snapshot at the same moment you stop processing events for that aggregate. this event should point to the backup file. so it can be used when restoring the snapshot.


 The restore an snapshot is also very simple
 aggregate.restore(snapshotName)
 - find backup files using snapshotName and locate snapshot event in the event store.
 - ** at this stage the event store might be receiving new events -> write is enabled **
 - backup is restored.
 - read is enabled :)
 - events start processing from the snapshot moment
 - ** system takes a while to catch up **
 - system is eventually consistent :)
