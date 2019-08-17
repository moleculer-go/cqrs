package cqrs

import (
	"errors"
	"fmt"

	"github.com/moleculer-go/moleculer"
	"github.com/moleculer-go/moleculer/util"
	"github.com/moleculer-go/store"
	log "github.com/sirupsen/logrus"
)

// Aggregate returns an aggregator which contain functions such as (Create, )
func Aggregate(name string, storeFactory StoreFactory, snapshotStrategy SnapshotStrategy) Aggregator {
	backup, restore := snapshotStrategy(name)
	return &aggregator{name: name, storeFactory: storeFactory, backup: backup, restore: restore, transformers: map[string][]string{}}
}

type aggregator struct {
	name string

	storeFactory  StoreFactory
	store         store.Adapter
	serviceSchema moleculer.ServiceSchema

	logger        *log.Entry
	brokerContext moleculer.BrokerContext
	parentService moleculer.ServiceSchema

	eventStore     EventStorer // remove this.. use the eventStoreName.  this creates coupling between the event store and the aggregate.. it should all be through actions.
	eventStoreName string

	backup  BackupStrategy
	restore RestoreStrategy

	//transformers map events, to a list of transformation actions.
	transformers map[string][]string
}

type actionMapper struct {
	eventName    string
	eventHandler moleculer.EventHandler
	onEventName  func(eventName string)
}

func (am *actionMapper) From(eventName string) moleculer.Event {
	am.eventName = eventName
	return moleculer.Event{
		Name:    eventName,
		Handler: am.eventHandler,
	}
}

// Mixin return the mixin schema for CQRS plugin
func (a *aggregator) Mixin() moleculer.Mixin {
	return moleculer.Mixin{
		Name:    "aggregate-mixin",
		Started: a.parentServiceStarted,
	}
}

// parentServiceStarted parent service started.
func (a *aggregator) parentServiceStarted(c moleculer.BrokerContext, svc moleculer.ServiceSchema) {
	c.Logger().Debug("parentServiceStarted()... ")
	a.brokerContext = c
	a.parentService = svc
	a.logger = c.Logger().WithField("aggregate", a.name)

	a.createServiceSchema()
	c.Logger().Debug("parentServiceStarted() before publishing service: ", a.serviceSchema.Name)
	c.Publish(a.serviceSchema)
	c.WaitFor(a.serviceSchema.Name)
	c.Logger().Debug("parentServiceStarted() service: ", a.serviceSchema.Name, " was published!")
}

func (a *aggregator) settings() map[string]interface{} {
	setts, ok := a.parentService.Settings["aggregate"]
	if ok {
		sm, ok := setts.(map[string]interface{})
		if ok {
			return sm
		}
	}
	return map[string]interface{}{}
}

// createServiceSchema create the aggregate service schema.
func (a *aggregator) createServiceSchema() {
	a.store = a.storeFactory(a.name, a.fields(), a.settings())
	a.serviceSchema = moleculer.ServiceSchema{
		Name:   a.name,
		Mixins: []moleculer.Mixin{store.Mixin(a.store)},
		Actions: []moleculer.Action{
			{
				Name:    "snapshot",
				Handler: a.snapshotAction,
			},
			{
				Name:    "restore",
				Handler: a.restoreAction,
			},
			{
				Name:    "restoreAndReplay",
				Handler: a.restoreAndReplayAction,
			},
			{
				Name:    "replay",
				Handler: a.replayAction,
			},
		},
	}
}

// Create receives an transformer and returns a EventHandler.
// The event handler will create an aggregate record in the aggregate store.
func (a *aggregator) Create(transformAction string) ActionMaping {
	return &actionMapper{
		eventHandler: func(c moleculer.Context, event moleculer.Payload) {
			eventID := event.Get("id").String()
			record := <-c.Call(transformAction, event)
			if record.IsError() {
				c.Logger().Error(a.name+".Create() Could not transform event - eventId: ", eventID, " - error: ", record.Error())
				c.Emit(a.name+".created.error", record)
				return
			}
			c.Call(a.name+".create", record.Add("eventId", eventID))
		},
		onEventName: func(eventName string) {
			a.transformers[eventName] = append(a.transformers[eventName], transformAction)
		},
	}
}

// CreateMany receives an transformer and returns an EventHandler.
// The event handler will create multiple  aggregate records in the aggregate store.
func (a *aggregator) CreateMany(transformAction string) ActionMaping {
	return &actionMapper{
		eventHandler: func(c moleculer.Context, event moleculer.Payload) {
			eventID := event.Get("id").String()
			records := <-c.Call(transformAction, event)
			for _, record := range records.Array() {
				if record.IsError() {
					c.Logger().Error("Aggregate.CreateMany() Could not transform event - eventId: ", eventID, " - error: ", record.Error())
					c.Emit(a.name+".create.error", record)
					continue
				}
				c.Call(a.name+".create", record.Add("eventId", eventID))
			}
		},
		onEventName: func(eventName string) {
			//Design: we shuold also store this mapping in the metadata that is sent / saved in the snapshot event.
			//this way instead of relying on the in-memory mapping.. we use the metadata..to invoke the transform actions :)
			a.transformers[eventName] = append(a.transformers[eventName], transformAction)
		},
	}
}

// Update receives an transformer and returns an EventHandler.
// The event handler will update an existing aggregate record in the aggregate store.
func (a *aggregator) Update(transformAction string) ActionMaping {
	return &actionMapper{
		eventHandler: func(c moleculer.Context, event moleculer.Payload) {
			eventID := event.Get("id").String()
			record := <-c.Call(transformAction, event)
			if record.IsError() {
				c.Logger().Error(a.name+".Update() Could not transform event - eventId: ", eventID, " - error: ", record.Error())
				c.Emit(a.name+".updated.error", record)
				return
			}
			if record.Get("id").Exists() {
				c.Call(a.name+".update", record.Add("eventId", eventID))
			} else {
				c.Call(a.name+".create", record.Add("eventId", eventID))
			}
		},
		onEventName: func(eventName string) {
			a.transformers[eventName] = append(a.transformers[eventName], transformAction)
		},
	}
}

// fields return a map with fields that this event store needs in the store
func (a *aggregator) fields() map[string]interface{} {
	return map[string]interface{}{
		"eventId": "integer",
	}
}

//Snapshot setup the snashot options for this aggregate.
func (a *aggregator) Snapshot(eventStore EventStorer) Aggregator {
	a.eventStore = eventStore
	return a
}

//snapshotAction - snapshot the aggregate
// - pause event pumps -> pause aggregate changes :)
//  - create a snapshot event -> new events will pile up after this point! = While Write is enabled.

//  - ** no changes are happening on aggregates ** while reads continues happily.
//  - backup aggregates -> aggregate.backup(snapshotID)  (SQLLite -> basicaly copy files :) )
//  - restart the event pump :)
//  - done.
//  - Error scenarios:
//  - if backup fails the event is marked as failed and is ignored when trying to restore events.
//  - Rationale:
//  ---> Since you created an event about the start of the snapshot at the same moment you paused the pump. this event should point to the backup file. so it can be used when restoring the snapshot.
func (a *aggregator) snapshotAction(context moleculer.Context, params moleculer.Payload) interface{} {
	if a.eventStore == nil {
		return errors.New("snapshot not configured for this aggregate. eventStore is nil")
	}

	//snapshot the aggregate
	snapshotID := "snapshot_" + util.RandomString(12)
	aggregateMetadata := map[string]interface{}{
		"transformers": a.transformers,
	}

	err := a.eventStore.StartSnapshot(snapshotID, aggregateMetadata)
	if err != nil {
		// error creating the snapshot event
		return errors.New("aggregate.snapshot action failed. We could not create the snapshot event. Error: " + err.Error())
	}
	err = a.backup(snapshotID)
	if err != nil {
		a.eventStore.FailSnapshot(snapshotID)
		// error creating the backup
		return errors.New("aggregate.snapshot action failed. We could not backup the aggregate. Error: " + err.Error())
	}

	err = a.eventStore.CompleteSnapshot(snapshotID)
	if err != nil {
		return errors.New("aggregate.snapshot action failed. We could not create the snapshot complete event. Error: " + err.Error())
	}
	return snapshotID
}

// restore a snapshot (backup) data back into the aggregate
func (a *aggregator) restoreAction(context moleculer.Context, params moleculer.Payload) interface{} {
	snapshotID := params.Get("snapshotID").String()

	err := a.eventStore.PauseEvents()
	if err != nil {
		return errors.New("aggregate.restore action failed. We could not create stop the event pump. Error: " + err.Error())
	}
	err = a.restore(snapshotID)
	if err != nil {
		return errors.New("aggregate.restore action failed. We could not restore the aggregate backup. Error: " + err.Error())
	}
	a.eventStore.StartEvents()
	return snapshotID
}

func (a *aggregator) restoreAndReplayAction(context moleculer.Context, params moleculer.Payload) interface{} {
	return nil
}

//may this should not be an aggregate responsability.. the event store must expose a replay API.
//replayAction replays the events
func (a *aggregator) replayAction(context moleculer.Context, params moleculer.Payload) interface{} {
	snapshot := <-context.Call(a.eventStoreName+"getSnapshot", params.Only("snapshotID"))
	snapshotCreated := snapshot.Get("created").Time()
	aggregateMetadata := snapshot.Get("aggregateMetadata")
	transformers := aggregateMetadata.Get("transformers")

	eventNames := []string{}
	transformers.ForEach(func(key interface{}, value moleculer.Payload) bool {
		eventNames = append(eventNames, key.(string))
		return true
	})

	events := <-context.Call(a.eventStore.Name()+".list", M{"created": M{">": snapshotCreated}})

	fmt.Println(events)

	return nil
}

//eventsSince return all events since snapshot
func (a *aggregator) eventsSince(snapshotID string) moleculer.Payload {

	// snaphot := a.brokerContext.Call(a.eventStore.Name()+".list", M{
	// 	"query": M{"status": StatusComplete, "eventType": TypeCommand},
	// })
	// morePages := true
	// for {
	// 	if !morePages {
	// 		break
	// 	}

	// 	eventPage := a.brokerContext.Call(a.eventStore.Name()+".list", M{
	// 		"query": M{"status": StatusComplete, "eventType": TypeCommand},
	// 		"sort":  "created",
	// 		"limit": limit,
	// 	})
	// 	if events.Len() > 0 {
	// 		return events
	// 	}

	// }]
	return nil
}
