package cqrs

import (
	"errors"
	"fmt"

	"github.com/moleculer-go/moleculer/payload"

	"github.com/moleculer-go/moleculer"
	"github.com/moleculer-go/moleculer/util"
	"github.com/moleculer-go/store"
	log "github.com/sirupsen/logrus"
)

// Aggregate returns an aggregator which contain functions such as (Create, )
func Aggregate(name string, storeFactory StoreFactory, snapshotStrategy SnapshotStrategy) Aggregator {
	backup, restore := snapshotStrategy(name)
	return &aggregator{name: name, storeFactory: storeFactory, backup: backup, restore: restore}
}

type aggregator struct {
	name string

	storeFactory  StoreFactory
	store         store.Adapter
	serviceSchema moleculer.ServiceSchema

	logger        *log.Entry
	brokerContext moleculer.BrokerContext
	parentService moleculer.ServiceSchema

	eventStoreName string

	backup  BackupStrategy
	restore RestoreStrategy

	eventMappings []eventMapper

	processEvents bool
}

type eventMapper struct {
	eventName       string
	aggregateName   string
	transformAction string
	aggregateAction string
	isActive        func() bool
}

func (a *aggregator) On(eventName string) EventMapping {
	return &eventMapper{aggregateName: a.name, eventName: eventName, isActive: func() bool { return a.processEvents }}
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
	a.processEvents = true

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
		Started: func(c moleculer.BrokerContext, s moleculer.ServiceSchema) {
			fmt.Println("####### -> " + a.name + " started!")
		},
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
			{
				Name:    "transformAndCreate",
				Handler: a.transformAndCreateAction,
			},
			{
				Name:    "transformAndCreateMany",
				Handler: a.transformAndCreateManyAction,
			},
			{
				Name:    "transformAndUpdate",
				Handler: a.transformAndUpdateAction,
			},
		},
	}
}

//transformAndCreateAction receives a transform action, an event and an aggregate name,
// which the create will be applied.
//It calls the transform action with the event, and with the result creates a aggregate record.
func (a *aggregator) transformAndCreateAction(c moleculer.Context, params moleculer.Payload) interface{} {
	transformAction := params.Get("transformAction").String()
	event := params.Get("event")
	eventID := event.Get("id").String()

	record := <-c.Call(transformAction, event)
	if record.IsError() {
		c.Logger().Error(a.name+" [Aggregate] Create() Error. Could not transform event - eventId: ", eventID, " - error: ", record.Error())
		c.Emit(a.name+".create.transform_error", record.Add("event", event))
		return record
	}
	result := <-c.Call(a.name+".create", record.Add("eventId", eventID))
	if result.IsError() {
		c.Emit(a.name+".create.error", result.Add("event", event))
		return record
	}
	return result
}

// Create receives an transform action and returns an moleculer event.
// The event handler will create an aggregate record in the aggregate store with the result of the transformation.
func (em *eventMapper) Create(transformAction string) moleculer.Event {
	em.transformAction = transformAction
	em.aggregateAction = "transformAndCreate"
	return moleculer.Event{
		Name:  em.eventName,
		Group: em.transformAction,
		Handler: func(c moleculer.Context, event moleculer.Payload) {
			if !em.isActive() {
				return
			}
			action := em.aggregateName + "." + em.aggregateAction
			c.Call(
				action,
				payload.Empty().
					Add("event", event).
					Add("transformAction", em.transformAction))
		},
	}
}

//transformAndCreateManyAction receives a transform action, an event and an aggregate name,
// which the create will be applied.
//It calls the transform action with the event, and with the result creates a aggregate record.
func (a *aggregator) transformAndCreateManyAction(c moleculer.Context, params moleculer.Payload) interface{} {
	transformAction := params.Get("transformAction").String()
	event := params.Get("event")
	eventID := event.Get("id").String()

	records := <-c.Call(transformAction, event)
	results := []moleculer.Payload{}
	for _, record := range records.Array() {
		if record.IsError() {
			c.Logger().Error(a.name+" [Aggregate] CreateMany() Could not transform event - eventId: ", eventID, " - error: ", record.Error())
			c.Emit(a.name+".create.transform_error", record.Add("event", event))
			continue
		}
		result := <-c.Call(a.name+".create", record.Add("eventId", eventID))
		if result.IsError() {
			c.Emit(a.name+".create.error", result.Add("event", event))
			continue
		}
		results = append(results, result)
	}
	return results
}

// CreateMany receives an transformer and returns an EventHandler.
// The event handler will create multiple  aggregate records in the aggregate store.
func (em *eventMapper) CreateMany(transformAction string) moleculer.Event {
	em.transformAction = transformAction
	em.aggregateAction = "transformAndCreateMany"
	return moleculer.Event{
		Name:  em.eventName,
		Group: em.transformAction,
		Handler: func(c moleculer.Context, event moleculer.Payload) {
			if !em.isActive() {
				return
			}
			c.Call(
				em.aggregateName+"."+em.aggregateAction,
				payload.Empty().
					Add("event", event).
					Add("transformAction", em.transformAction))
		},
	}
}

func (a *aggregator) transformAndUpdateAction(c moleculer.Context, params moleculer.Payload) interface{} {
	transformAction := params.Get("transformAction").String()
	event := params.Get("event")
	eventID := event.Get("id").String()

	record := <-c.Call(transformAction, event)
	if record.IsError() {
		c.Logger().Error(a.name+" [Aggregate] Update() Could not transform event - eventId: ", eventID, " - error: ", record.Error())
		c.Emit(a.name+".update.transform_error", record.Add("event", event))
		return record
	}
	if record.Get("id").Exists() {
		result := <-c.Call(a.name+".update", record.Add("eventId", eventID))
		if result.IsError() {
			c.Emit(a.name+".update.error", result.Add("event", event))
		}
		return result
	} else {
		result := <-c.Call(a.name+".create", record.Add("eventId", eventID))
		if result.IsError() {
			c.Emit(a.name+".create.error", result.Add("event", event))
		}
		return result
	}
}

// Update receives an transformer and returns an EventHandler.
// The event handler will update an existing aggregate record in the aggregate store.
func (em *eventMapper) Update(transformAction string) moleculer.Event {
	em.transformAction = transformAction
	em.aggregateAction = "transformAndUpdate"
	return moleculer.Event{
		Name:  em.eventName,
		Group: em.transformAction,
		Handler: func(c moleculer.Context, event moleculer.Payload) {
			if !em.isActive() {
				return
			}
			c.Call(
				em.aggregateName+"."+em.aggregateAction,
				payload.Empty().
					Add("event", event).
					Add("transformAction", em.transformAction))
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
func (a *aggregator) Snapshot(eventStore string) Aggregator {
	a.eventStoreName = eventStore
	return a
}

//PAREI AQUI... add aggregate action in the transformers map.. and use snapshotIDit to replay the events.
//transformers returns a map of events to a list of transformation actions -> map[string][]string
func (a *aggregator) transformers() map[string][]string {
	transformers := map[string][]string{}
	for _, em := range a.eventMappings {
		transformers[em.eventName] = append(transformers[em.eventName], em.transformAction)
	}
	return transformers
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
	if a.eventStoreName == "" {
		return errors.New("snapshot not configured for this aggregate. eventStore is nil")
	}

	//snapshot the aggregate
	snapshotID := "snapshot_" + util.RandomString(5)

	r := <-context.Call(a.eventStoreName+".startSnapshot", M{
		"snapshotID": snapshotID,
		"aggregateMetadata": M{
			"transformers": a.transformers(),
		},
	})
	if r.IsError() {
		// error creating the snapshot event
		return errors.New("aggregate.snapshot action failed. We could not create the snapshot event. Error: " + r.Error().Error())
	}
	err := a.backup(snapshotID)
	if err != nil {
		<-context.Call(a.eventStoreName+".failSnapshot", M{"snapshotID": snapshotID})
		// error creating the backup
		return errors.New("aggregate.snapshot action failed. We could not backup the aggregate. Error: " + err.Error())
	}

	r = <-context.Call(a.eventStoreName+".completeSnapshot", M{"snapshotID": snapshotID})
	if r.IsError() {
		return errors.New("aggregate.snapshot action failed. We could not create the snapshot complete event. Error: " + r.Error().Error())
	}
	return snapshotID
}

// stopEvents stop the aggregate from processing incoming events.
// it has effect on this aggregate instance. Need to investigate if is a good idea to broadcast an event
// and use the event o change the flag.. this way multiple instances of the same aggregate can pause and restart
// the issue with that is fire and forget.. there is no guarantee that all have paused.
//. for now.. for simple deployment and to complete the feature and its tests single instance is enough.
func (a *aggregator) stopEvents() {
	a.processEvents = false
}

func (a *aggregator) startEvents() {
	a.processEvents = true
}

// restore a snapshot (backup) data back into the aggregate
func (a *aggregator) restoreAction(context moleculer.Context, params moleculer.Payload) interface{} {
	snapshotID := params.Get("snapshotID").String()

	a.stopEvents()

	err := a.restore(snapshotID)
	if err != nil {
		return errors.New("aggregate.restore action failed. We could not restore the aggregate backup. Error: " + err.Error())
	}

	a.startEvents()
	return snapshotID
}

func (a *aggregator) restoreAndReplayAction(context moleculer.Context, params moleculer.Payload) interface{} {
	snapshotID := params.Get("snapshotID").String()

	a.stopEvents()

	err := a.restore(snapshotID)
	if err != nil {
		return errors.New("aggregate.restore action failed. We could not restore the aggregate backup. Error: " + err.Error())
	}

	a.replayAction(context, params)

	a.startEvents()
	return snapshotID
}

//replayAction replays the all events since a specific snapshotID
func (a *aggregator) replayAction(context moleculer.Context, params moleculer.Payload) interface{} {

	params = params.Only("snapshotID")
	snapshot := <-context.Call(a.eventStoreName+".getSnapshot", params)

	page := 1
	pageSize := 100
	total := 0
	params = params.Add("pageSize", pageSize)
	for {
		batch := a.eventsSince(context, snapshot, params.Add("page", page).Add("total", total))
		if batch.IsError() {
			return batch.Error()
		}
		a.replayEvents(context, snapshot, batch.Get("rows"))
		if batch.Get("page").Int() >= batch.Get("totalPages").Int() || batch.Get("rows").Len() == 0 {
			break
		}
		total = params.Get("total").Int()
		page = batch.Get("page").Int() + 1
	}

	return nil
}

//replayEvents invoke all transformation actions and aggregate action for each event, in the list of events.
func (a *aggregator) replayEvents(context moleculer.Context, snapshot, events moleculer.Payload) {
	events.ForEach(func(_ interface{}, event moleculer.Payload) bool {
		aggregateMetadata := snapshot.Get("aggregateMetadata")
		transformers := aggregateMetadata.Get("transformers")
		eventName := event.Get("event").String()
		actions := transformers.Get(eventName)
		if !actions.Exists() || actions.Len() == 0 {
			return true
		}
		actions.ForEach(func(_ interface{}, action moleculer.Payload) bool {
			<-context.Call(action.String(), event)
			return true
		})
		return true
	})
}

//eventsSince fetch all events since snapshot ordered by the created date with pagination (pageSize and page params)
func (a *aggregator) eventsSince(context moleculer.Context, snapshot, params moleculer.Payload) moleculer.Payload {
	snapshotCreated := snapshot.Get("created").Time()
	aggregateMetadata := snapshot.Get("aggregateMetadata")
	transformers := aggregateMetadata.Get("transformers")

	pageSize := params.Get("pageSize").Int()
	page := params.Get("page").Int()
	total := params.Get("total").Int()

	eventNames := []string{}
	transformers.ForEach(func(key interface{}, value moleculer.Payload) bool {
		eventNames = append(eventNames, key.(string))
		return true
	})

	return <-context.Call(a.eventStoreName+".find", M{
		"query": M{
			"eventType": TypeCommand,
			"status":    StatusComplete,
			"event":     M{"in": eventNames},
			"created":   M{">=": snapshotCreated},
		},
		"sort":   "created",
		"total":  total,
		"offset": (page - 1) * pageSize,
		"limit":  page * pageSize,
	})
}
