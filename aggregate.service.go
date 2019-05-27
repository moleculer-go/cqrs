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
	c.Logger().Debug("parentServiceStarted... ")
	a.brokerContext = c
	a.parentService = svc
	a.logger = c.Logger().WithField("aggregate", a.name)

	a.createServiceSchema()
	c.Logger().Debug("parentServiceStarted before publishing service: ", a.serviceSchema.Name)
	c.Publish(a.serviceSchema)
	c.WaitFor(a.serviceSchema.Name)
	c.Logger().Debug("parentServiceStarted service published! service: ", a.serviceSchema.Name)
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
	adapter := a.createAdapter(a.name, a.fields(), a.settings())
	a.serviceSchema = moleculer.ServiceSchema{
		Name:   a.name,
		Mixins: []moleculer.Mixin{store.Mixin(adapter)},
	}
}

// Create receives an transformer and returns a EventHandler.
// The event handler will create an aggregate record in the aggregate store.
func (a *aggregator) Create(transform Transformer) moleculer.EventHandler {
	return func(c moleculer.Context, event moleculer.Payload) {
		eventId := event.Get("id").String()
		record := transform(c, event)
		if record.IsError() {
			c.Logger().Error(a.name+".Create() Could not transform event - eventId: ", eventId, " - error: ", record.Error())
			c.Emit(a.name+".created.error", record)
			return
		}
		c.Call(a.name+".create", record.Add("eventId", eventId))
	}
}

// CreateMany receives an transformer and returns a EventHandler.
// The event handler will create multiple  aggregate records in the aggregate store.
func (a *aggregator) CreateMany(transform ManyTransformer) moleculer.EventHandler {
	return func(c moleculer.Context, event moleculer.Payload) {
		eventId := event.Get("id").String()
		records := transform(c, event)
		for _, record := range records {
			if record.IsError() {
				c.Logger().Error("Aggregate.CreateMany() Could not transform event - eventId: ", eventId, " - error: ", record.Error())
				c.Emit(a.name+".create.error", record)
				continue
			}
			c.Call(a.name+".create", record.Add("eventId", eventId))
		}
	}
}

// fields return a map with fields that this event store needs in the adapter
func (a *aggregator) fields() map[string]interface{} {
	return map[string]interface{}{
		"eventId": "integer",
	}
}
