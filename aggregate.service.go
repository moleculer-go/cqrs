package cqrs

import (
	"github.com/moleculer-go/moleculer"
	"github.com/moleculer-go/store"
	log "github.com/sirupsen/logrus"
)

// Aggregate returns an aggregator which contain functions such as (Create, )
func Aggregate(name string, createAdapter AdapterFactory) Aggregator {
	return &aggregator{name: name, createAdapter: createAdapter}
}

type aggregator struct {
	name          string
	createAdapter AdapterFactory
	serviceSchema moleculer.ServiceSchema

	logger        *log.Entry
	brokerContext moleculer.BrokerContext
	parentService moleculer.ServiceSchema
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
	a.brokerContext = c
	a.parentService = svc
	a.logger = c.Logger().WithField("aggregate", a.name)

	a.createServiceSchema()
	c.Publish(a.serviceSchema)
	c.WaitFor(a.serviceSchema.Name)
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
	//TODO add the field eventId
	adapter := a.createAdapter(a.name+"Aggregate", a.fields(), a.settings())
	a.serviceSchema = moleculer.ServiceSchema{
		Name:   a.name,
		Mixins: []moleculer.Mixin{store.Mixin(adapter)},
		Started: func(c moleculer.BrokerContext, svc moleculer.ServiceSchema) {

		},
	}
}

// Create received an transformer and returns a EventHandler.
// The event handler will create an aggregate record in the aggregate store.
func (a *aggregator) Create(transform Transformer) moleculer.EventHandler {
	return func(c moleculer.Context, event moleculer.Payload) {
		eventID := event.Get("id").String()
		record := transform(c, event)
		if record.IsError() {
			c.Logger().Error("EventStore.Create() Could not transform event - eventID: ", eventID, " - error: ", record.Error())
			c.Emit("property.created.error", record)
			return
		}
		record = a.create(c, record.Add("eventID", eventID))
		c.Emit("property.created.successfully", record)
	}
}

//create create a new aggregate record.
// it has already being validated and transformed.
func (a *aggregator) create(c moleculer.Context, r moleculer.Payload) moleculer.Payload {
	//TODO save record to the model
	return r
}

// fields return a map with fields that this event store needs in the adapter
func (a *aggregator) fields() map[string]interface{} {
	return map[string]interface{}{
		"eventID": "integer",
	}
}
