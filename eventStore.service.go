package cqrs

import (
	"time"

	"github.com/moleculer-go/moleculer"
	"github.com/moleculer-go/moleculer/payload"
	"github.com/moleculer-go/moleculer/serializer"
	"github.com/moleculer-go/store"
	log "github.com/sirupsen/logrus"
)

const (
	StatusCreated    = 0
	StatusProcessing = 1
	StatusComplete   = 2
	StatusFailed     = 3
	StatusRetrying   = 4
)

func EventStore(name string, createAdapter AdapterFactory, fields ...map[string]interface{}) EventStorer {
	return &eventStore{
		name:          name,
		createAdapter: createAdapter,
		extraFields:   mergeMapsList(fields),
	}
}

type eventStore struct {
	name              string
	eventStoreService moleculer.ServiceSchema
	eventStoreAdapter store.Adapter
	createAdapter     AdapterFactory

	extraFields map[string]interface{}

	brokerContext     moleculer.BrokerContext
	parentService     moleculer.ServiceSchema
	serializer        serializer.Serializer
	logger            *log.Entry
	stopping          bool
	dispatchBatchSize int
	poolingInterval   time.Duration
}

// Mixin return the mixin schema for CQRS plugin
func (e *eventStore) Mixin() moleculer.Mixin {
	return moleculer.Mixin{
		Name:    "eventStore-mixin",
		Started: e.parentServiceStarted,
	}
}

//storeServiceStarted event store started.
func (e *eventStore) storeServiceStarted(c moleculer.BrokerContext, svc moleculer.ServiceSchema) {
	e.eventStoreService = svc
	//TODO get from settings
	e.dispatchBatchSize = 10
	e.poolingInterval = time.Second
	go e.dispatchEvents()
}

//storeServiceStopped
func (e *eventStore) storeServiceStopped(c moleculer.BrokerContext, svc moleculer.ServiceSchema) {
	e.stopping = true
}

//fetchNextEvent return the next event to be processed.
// blocks until there is a event available.
// also checks for retry events.
func (e *eventStore) fetchNextEvents(limit int) moleculer.Payload {
	for {
		events := e.eventStoreAdapter.FindAndUpdate(payload.New(map[string]interface{}{
			"query":  map[string]interface{}{"status": StatusCreated},
			"sort":   "created -version",
			"limit":  limit,
			"update": map[string]interface{}{"status": StatusProcessing},
		}))
		if events.Len() > 0 {
			return events
		}
		if e.stopping {
			return payload.Empty()
		}
		time.Sleep(e.poolingInterval)
	}
}

//dispatchEvents read events from the store and dispatch events.
func (e *eventStore) dispatchEvents() {
	for {
		events := e.fetchNextEvents(e.dispatchBatchSize)
		for _, event := range events.Array() {
			e.brokerContext.Emit(event.Get("event").String(), event)
			e.eventStoreAdapter.Update(payload.New(map[string]interface{}{
				"id":      event.Get("id"),
				"status":  StatusComplete,
				"updated": time.Now().Unix(),
			}))
			if e.stopping {
				return
			}
		}
	}
}

// parentServiceStarted parent service started.
func (e *eventStore) parentServiceStarted(c moleculer.BrokerContext, svc moleculer.ServiceSchema) {
	e.brokerContext = c
	e.parentService = svc
	e.logger = c.Logger().WithField("eventStore", e.name)
	e.serializer = serializer.CreateJSONSerializer(e.logger)
	e.createEventStoreService()
	c.Publish(e.eventStoreService)
	c.WaitFor(e.eventStoreService.Name)
}

func (e *eventStore) settings() map[string]interface{} {
	setts, ok := e.parentService.Settings["cqrs"]
	if ok {
		sm, ok := setts.(map[string]interface{})
		if ok {
			return sm
		}
	}
	return map[string]interface{}{}
}

func (e *eventStore) createEventStoreService() {
	name := e.name + "EventStore"
	e.eventStoreAdapter = e.createAdapter(name, e.fields(), e.settings())
	e.eventStoreService = moleculer.ServiceSchema{
		Name:    name,
		Mixins:  []moleculer.Mixin{store.Mixin(e.eventStoreAdapter)},
		Started: e.storeServiceStarted,
		Stopped: e.storeServiceStopped,
	}
}

// NewEvent receives eventName and extraParams and returns an action handler
// that saves the payload as an event record inside the event store.
// extraParams are label=value to be saved in the event record.
// if it fails to save the event to the store it emits the event eventName.failed
func (e *eventStore) NewEvent(eventName string, extraParams ...map[string]interface{}) moleculer.ActionHandler {
	return func(c moleculer.Context, p moleculer.Payload) interface{} {
		event := map[string]interface{}{
			"event":   eventName,
			"created": time.Now().Unix(),
			"status":  StatusCreated,
		}
		//merge event with params
		extra := e.parseExtraParams(extraParams)
		if len(extra) > 0 {
			for name, value := range extra {
				event[name] = value
			}
		}
		event["payload"] = e.serializer.PayloadToBytes(p)

		//save to the event store
		r := <-c.Call(e.eventStoreService.Name+".create", event)
		if r.IsError() {
			c.Emit(
				eventName+".failed",
				payload.Empty().Add("error", r).Add("event", event),
			)
			return r
		}
		return r
	}
}

// parseExtraParams extract valid extra parameters
func (e *eventStore) parseExtraParams(params []map[string]interface{}) map[string]interface{} {
	if len(params) > 0 {
		extra := map[string]interface{}{}
		for _, item := range params {
			for name, value := range item {
				extra[name] = value
			}
		}
		if len(extra) > 0 && e.validExtras(extra) {
			return extra
		}
	}
	return map[string]interface{}{}
}

// validExtras check if all fields in the extras map are valid
func (e *eventStore) validExtras(extras map[string]interface{}) bool {
	for extra := range extras {
		if _, ok := e.extraFields[extra]; ok {
			return true
		}
	}
	return false
}

// fields return a map with fields that this event store needs in the adapter
func (e *eventStore) fields() map[string]interface{} {
	f := map[string]interface{}{
		"event":      "string",
		"version":    "integer",
		"created":    "integer",
		"updated":    "integer",
		"status":     "integer",
		"payload":    "[]byte",
		"aggregates": "[]string",
	}
	if len(e.extraFields) > 0 {
		for k, v := range e.extraFields {
			f[k] = v
		}
	}
	return f
}

func mergeMapsList(ms []map[string]interface{}) map[string]interface{} {
	r := map[string]interface{}{}
	for _, m := range ms {
		for k, v := range m {
			r[k] = v
		}
	}
	return r
}
