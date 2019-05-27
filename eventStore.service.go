package cqrs

import (
	"fmt"
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

func EventStore(name string, createAdapter AdapterFactory, fields ...M) EventStorer {
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

// Mixin return the mixin schema for the eventStore plugin
func (e *eventStore) Mixin() moleculer.Mixin {
	return moleculer.Mixin{
		Name:    "eventStore-mixin",
		Started: e.parentServiceStarted,
		Actions: []moleculer.Action{
			{
				Name:    "$startDispatch",
				Handler: e.startDispatch,
			},
			{
				//TODO make actions prefixed with $ available only in then local broker.
				//so they are never published to other brokers.
				Name:    "$stopDispatch",
				Handler: e.stopDispatch,
			},
		},
	}
}

func (e *eventStore) getSetting(name string, defaultValue interface{}) interface{} {
	value, exists := e.settings()[name]
	if exists {
		return value
	}
	return defaultValue
}

//storeServiceStarted event store started.
func (e *eventStore) storeServiceStarted(c moleculer.BrokerContext, svc moleculer.ServiceSchema) {
	e.eventStoreService = svc
	e.dispatchBatchSize = e.getSetting("dispatchBatchSize", 1).(int)
	e.poolingInterval = e.getSetting("poolingInterval", time.Microsecond).(time.Duration)
	go e.dispatchEvents()
}

//storeServiceStopped
func (e *eventStore) storeServiceStopped(c moleculer.BrokerContext, svc moleculer.ServiceSchema) {
	e.stopping = true
	c.Logger().Debug("eventStore storeServiceStopped() called...")
}

type M map[string]interface{}

//fetchNextEvent return the next event to be processed.
// blocks until there is a event available.
// also checks for retry events.
func (e *eventStore) fetchNextEvents(limit int) moleculer.Payload {
	fmt.Println("fetchNextEvents limit: ", limit)
	for {
		if e.stopping {
			return payload.EmptyList()
		}
		events := e.eventStoreAdapter.FindAndUpdate(payload.New(M{
			"query": M{"status": StatusCreated},
			"sort":  "created",
			"limit": limit,
			"update": M{
				"status":  StatusProcessing,
				"updated": time.Now().Unix(),
			},
		}))
		if events.Len() > 0 {
			return events
		}
		time.Sleep(e.poolingInterval)
	}
}

// prepareEventForEmit prepare the even record to be sent over to all consumers!
// It transforms the contentsL
// 1) Parse the payload []byte bytes back into a usable payload object.
func (e *eventStore) prepareEventForEmit(event moleculer.Payload) moleculer.Payload {
	bts := event.Get("payload").ByteArray()
	p := e.serializer.BytesToPayload(&bts)
	return event.Add("payload", p)
}

//dispatchEvents read events from the store and dispatch events.
func (e *eventStore) dispatchEvents() {
	for {
		if e.stopping {
			return
		}
		batch := e.fetchNextEvents(e.dispatchBatchSize)
		updated := time.Now().Unix()
		for _, event := range batch.Array() {
			event = e.prepareEventForEmit(event).Add("batchSize", batch.Len())
			e.brokerContext.Emit(event.Get("event").String(), event)
			e.eventStoreAdapter.Update(payload.New(M{
				"id":      event.Get("id"),
				"status":  StatusComplete,
				"updated": updated,
			}))
			if e.stopping {
				return
			}
		}
	}
}

func (e *eventStore) stopDispatch(c moleculer.Context, p moleculer.Payload) interface{} {
	e.stopping = true
	c.Logger().Debug("eventStore stopDispatch() called...")
	return nil
}

func (e *eventStore) startDispatch(c moleculer.Context, p moleculer.Payload) interface{} {
	if e.stopping == true {
		e.stopping = false
		c.Logger().Debug("eventStore startDispatch() called - starting dispatch pump!")
		go e.dispatchEvents()
	}
	return nil
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

func (e *eventStore) settings() M {
	setts, ok := e.parentService.Settings["eventStore"]
	if ok {
		s := payload.New(setts).RawMap()
		fmt.Println("settings: ", s)
		return s
	}
	return M{}
}

func (e *eventStore) createEventStoreService() {
	name := e.name + "EventStore"
	fieldMap := e.fields()
	e.eventStoreAdapter = e.createAdapter(name, fieldMap, e.settings())

	fields := []string{}
	for f := range fieldMap {
		fields = append(fields, f)
	}
	e.eventStoreService = moleculer.ServiceSchema{
		Name:   name,
		Mixins: []moleculer.Mixin{store.Mixin(e.eventStoreAdapter)},
		Settings: M{
			"fields": fields,
		},
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
		event := M{
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
		extra := M{}
		for _, item := range params {
			for name, value := range item {
				extra[name] = value
			}
		}
		if len(extra) > 0 && e.validExtras(extra) {
			return extra
		}
	}
	return M{}
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
func (e *eventStore) fields() M {
	f := M{
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

func mergeMapsList(ms []M) M {
	r := M{}
	for _, m := range ms {
		for k, v := range m {
			r[k] = v
		}
	}
	return r
}
