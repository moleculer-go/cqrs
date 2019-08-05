package cqrs

import (
	"errors"

	"github.com/moleculer-go/moleculer"
	"github.com/moleculer-go/moleculer/util"
	"github.com/moleculer-go/store"
	log "github.com/sirupsen/logrus"
)

// Aggregate returns an aggregator which contain functions such as (Create, )
func Aggregate(name string, storeFactory StoreFactory) Aggregator {
	return &aggregator{name: name, storeFactory: storeFactory}
}

type aggregator struct {
	name          string
	storeFactory  StoreFactory
	serviceSchema moleculer.ServiceSchema

	logger        *log.Entry
	brokerContext moleculer.BrokerContext
	parentService moleculer.ServiceSchema

	eventStore EventStorer
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
	adapter := a.storeFactory.Create(a.name, a.fields(), a.settings())
	a.serviceSchema = moleculer.ServiceSchema{
		Name:   a.name,
		Mixins: []moleculer.Mixin{store.Mixin(adapter)},
		Actions: []moleculer.Action{
			{
				Name:    "snapshot",
				Handler: a.snapshotAction,
			},
		},
	}
}

// Create receives an transformer and returns a EventHandler.
// The event handler will create an aggregate record in the aggregate store.
func (a *aggregator) Create(transform Transformer) moleculer.EventHandler {
	return func(c moleculer.Context, event moleculer.Payload) {
		eventID := event.Get("id").String()
		record := transform(c, event)
		if record.IsError() {
			c.Logger().Error(a.name+".Create() Could not transform event - eventId: ", eventID, " - error: ", record.Error())
			c.Emit(a.name+".created.error", record)
			return
		}
		c.Call(a.name+".create", record.Add("eventId", eventID))
	}
}

// CreateMany receives an transformer and returns an EventHandler.
// The event handler will create multiple  aggregate records in the aggregate store.
func (a *aggregator) CreateMany(transform ManyTransformer) moleculer.EventHandler {
	return func(c moleculer.Context, event moleculer.Payload) {
		eventID := event.Get("id").String()
		records := transform(c, event)
		for _, record := range records {
			if record.IsError() {
				c.Logger().Error("Aggregate.CreateMany() Could not transform event - eventId: ", eventID, " - error: ", record.Error())
				c.Emit(a.name+".create.error", record)
				continue
			}
			c.Call(a.name+".create", record.Add("eventId", eventID))
		}
	}
}

// Update receives an transformer and returns an EventHandler.
// The event handler will update an existing aggregate record in the aggregate store.
func (a *aggregator) Update(transform Transformer) moleculer.EventHandler {
	return func(c moleculer.Context, event moleculer.Payload) {
		eventID := event.Get("id").String()
		record := transform(c, event)
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
	}
}

// fields return a map with fields that this event store needs in the adapter
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

//Proposal
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

//snapshotAction - snapshot the aggregate
func (a *aggregator) snapshotAction(context moleculer.Context, params moleculer.Payload) interface{} {
	if a.eventStore == nil {
		return errors.New("snapshot not configured for this aggregate. eventStore is nil")
	}

	//snapshot the aggregate
	snapshotID := "snapshot_" + util.RandomString(12)
	aggregateMetadata := map[string]interface{}{}

	err := a.eventStore.StartSnapshot(snapshotID, aggregateMetadata)
	if err != nil {
		// error creating the snapshot event
		return errors.New("aggregate.snapshot action failed. We could not create the snapshot event. Error: " + err.Error())
	}
	err = a.storeFactory.Backup(snapshotID)
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
