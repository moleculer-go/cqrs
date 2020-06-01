package cqrs

import (
	"errors"
	"time"

	"github.com/moleculer-go/moleculer"
	"github.com/moleculer-go/moleculer/payload"
	"github.com/moleculer-go/moleculer/serializer"
	"github.com/moleculer-go/store"
	log "github.com/sirupsen/logrus"
)

const (
	TypeCommand           = 0
	TypeSnapshotCreated   = 1
	TypeSnapshotCompleted = 2
	TypeSnapshotFailed    = 3
)

const (
	StatusCreated    = 0
	StatusProcessing = 1
	StatusComplete   = 2
	StatusFailed     = 3
	StatusRetrying   = 4
)

func EventStore(name string, storeFactory StoreFactory, fields ...M) EventStorer {
	return &eventStore{
		name:         name,
		storeFactory: storeFactory,
		extraFields:  mergeMapsList(fields),
	}
}

type eventStore struct {
	name              string
	eventStoreService moleculer.ServiceSchema
	eventStoreAdapter store.Adapter
	storeFactory      StoreFactory

	extraFields map[string]interface{}

	brokerContext     moleculer.BrokerContext
	parentService     moleculer.ServiceSchema
	serializer        serializer.Serializer
	logger            *log.Entry
	stopping          bool
	dispatchBatchSize int
	poolingInterval   time.Duration

	dispatchEventsStopped bool
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

func (e *eventStore) Name() string {
	return e.name
}

//fetchNextEvent return the next event to be processed.
// blocks until there is a event available.
// also checks for retry events.
func (e *eventStore) fetchNextEvents(limit int) moleculer.Payload {
	for {
		if e.stopping {
			return payload.EmptyList()
		}
		events := e.eventStoreAdapter.FindAndUpdate(payload.New(M{
			"query": M{"status": StatusCreated, "eventType": TypeCommand},
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
	e.dispatchEventsStopped = false
	for {
		if e.stopping {
			e.dispatchEventsStopped = true
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
				e.dispatchEventsStopped = true
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

//TODO -> move this []byte to payload conversion to the adapter and make it a generic functionality :)..
//basically.. moleculer.payload when mapped to bytes in SQLite -> gets serialized
// and deserialized... using the serializer of choice. JSON is default.
func (e *eventStore) getSnapshotAction() moleculer.Action {
	return moleculer.Action{
		Name: "getSnapshot",
		Handler: func(c moleculer.Context, p moleculer.Payload) interface{} {
			snapshotID := p.Get("snapshotID").String()
			snapshot := <-c.Call(e.name+".findOne", M{"event": snapshotID})
			payload := snapshot.Get("payload").ByteArray()
			aggregateMetadata := e.serializer.BytesToPayload(&payload)
			return snapshot.Add("aggregateMetadata", aggregateMetadata)
		},
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

func (e *eventStore) settings() M {
	setts, ok := e.parentService.Settings["eventStore"]
	if ok {
		s := payload.New(setts).RawMap()
		return s
	}
	return M{}
}

func (e *eventStore) createEventStoreService() {
	fieldMap := e.fields()
	e.eventStoreAdapter = e.storeFactory(e.name, fieldMap, e.settings())

	fields := []string{}
	for f := range fieldMap {
		fields = append(fields, f)
	}
	e.eventStoreService = moleculer.ServiceSchema{
		Name:   e.name,
		Mixins: []moleculer.Mixin{store.Mixin(e.eventStoreAdapter)},
		Settings: M{
			"fields": fields,
		},
		Started: e.storeServiceStarted,
		Stopped: e.storeServiceStopped,
		Actions: []moleculer.Action{
			e.getSnapshotAction(),
			e.startSnapshotAction(),
			e.completeSnapshotAction(),
			e.failSnapshotAction(),
		},
	}
}

func (e *eventStore) saveEvent(c moleculer.Context, p moleculer.Payload, eventName string, extraParams ...map[string]interface{}) interface{} {
	event := M{
		"event":     eventName,
		"created":   time.Now().Unix(),
		"status":    StatusCreated,
		"eventType": TypeCommand,
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

// MapAction receives actionName, eventName and extraParams and returns an moleculer.Action with an action handler
// that saves the payload as an event record inside the event store.
// extraParams are label=value to be saved in the event record.
// if it fails to save the event to the store it emits the event eventName.failed
func (e *eventStore) MapAction(actionName, eventName string, extraParams ...map[string]interface{}) moleculer.Action {
	return moleculer.Action{
		Name: actionName,
		Handler: func(c moleculer.Context, p moleculer.Payload) interface{} {
			return e.saveEvent(c, p, eventName, extraParams...)
		},
	}
}

// MapEvent receives eventName and extraParams and returns an moleculer.Action with an action handler
// that saves the payload as an event record inside the event store.
// extraParams are label=value to be saved in the event record.
// if it fails to save the event to the store it emits the event eventName.failed
func (e *eventStore) MapEvent(eventName string, extraParams ...map[string]interface{}) moleculer.Event {
	return moleculer.Event{
		Name: eventName,
		Handler: func(c moleculer.Context, p moleculer.Payload) {
			e.saveEvent(c, p, eventName, extraParams...)
		},
	}
}

// 1.B) Alternative design: Instead of pausing/stoping the event pump, which can be related to many aggregates.. we should only pause the aggregate events.
//		the aggregate must pass on a filter criteria.. so the event pump can pause just the events that matches the filter and continue to serve other aggregates.
//		there could be aggreagates that share the same events, and there fore snapshot on aggreagte A can impact aggreagtee B if shares the same events, but the impact
// 		stops there.

// PauseEvents current implementatio pause event whole pump.
// 		change proposed is to have a filter, so we can pause only for certain events.
func (e *eventStore) PauseEvents() error {
	e.stopping = true
	for {
		if e.dispatchEventsStopped {
			break
		}
	}
	return nil
}

//StartEvents start event pump.
func (e *eventStore) StartEvents() {
	e.stopping = false
	e.logger.Debug("starting event pump!")
	go e.dispatchEvents()
}

// startSnapshotAction starts a snapshot:
// 1) Pause event pump , so no morechanges to aggreagates will be done.
// 1.B) Alternative design: Instead of pausing/stoping the event pump, which can be related to many aggregates.. we should only pause the aggregate events.
//		the aggregate must pass on a filter criteria.. so the event pump can pause just the events that matches the filter and continue to serve other aggregates.
//		there could be aggreagates that share the same events, and there fore snapshot on aggreagte A can impact aggreagtee B if shares the same events, but the impact
// 		stops there.
// 2) Create an event to record the snapshot, so it can be replayed from this point.
// Error Handling:
//  In case snapshotEvent fails, it restarts the pump and returns the error.
func (e *eventStore) startSnapshotAction() moleculer.Action {
	return moleculer.Action{
		Name: "startSnapshot",
		Handler: func(context moleculer.Context, params moleculer.Payload) interface{} {

			snapshotID := params.Get("snapshotID").String()
			aggregateMetadata := params.Get("aggregateMetadata").RawMap()

			e.logger.Debug("StartSnapshot snapshotID: ", snapshotID)
			//pause event pumps -> pause aggregate changes :)
			//e.PauseEvents()

			err := e.snapshotEvent(snapshotID, aggregateMetadata)
			if err != nil {
				//e.StartEvents()
				return err
			}
			return nil
		},
	}
}

// completeSnapshotAction complete a snapshot by resuming the event pump and
// recording an event to represent this.
func (e *eventStore) completeSnapshotAction() moleculer.Action {
	return moleculer.Action{
		Name: "completeSnapshot",
		Handler: func(c moleculer.Context, params moleculer.Payload) interface{} {
			snapshotID := params.Get("snapshotID").String()
			events := e.eventStoreAdapter.FindAndUpdate(payload.New(M{
				"query": M{"event": snapshotID, "eventType": TypeSnapshotCreated},
				"limit": 1,
				"update": M{
					"eventType": TypeSnapshotCompleted,
					"updated":   time.Now().Unix(),
				},
			}))
			if events.Len() < 1 {
				return errors.New("No snapshot found with id: " + snapshotID)
			}
			//e.StartEvents()
			return nil
		},
	}
}

// failSnapshotAction fails a snapshot by recording the failure in the event record.
func (e *eventStore) failSnapshotAction() moleculer.Action {
	return moleculer.Action{
		Name: "failSnapshot",
		Handler: func(c moleculer.Context, params moleculer.Payload) interface{} {
			snapshotID := params.Get("snapshotID").String()
			events := e.eventStoreAdapter.FindAndUpdate(payload.New(M{
				"query": M{"event": snapshotID, "eventType": TypeSnapshotCreated},
				"limit": 1,
				"update": M{
					"eventType": TypeSnapshotFailed,
					"updated":   time.Now().Unix(),
				},
			}))
			if events.Len() < 1 {
				return errors.New("No snapshot found with id: " + snapshotID)
			}
			//e.StartEvents()
			return nil
		},
	}
}

// snapshotEvent create an snapshot event in the event store and stores the aggregate metadata as payload.
func (e *eventStore) snapshotEvent(snapshotID string, aggregateMetadata map[string]interface{}) error {
	event := M{
		"event":     snapshotID,
		"created":   time.Now().Unix(),
		"status":    StatusComplete,
		"eventType": TypeSnapshotCreated,
		"payload":   e.serializer.PayloadToBytes(payload.New(aggregateMetadata)),
	}

	//save to the event store
	r := <-e.brokerContext.Call(e.eventStoreService.Name+".create", event)
	if r.IsError() {
		return r.Error()
	}
	return nil
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
		"event":     "string",
		"version":   "integer",
		"created":   "integer",
		"updated":   "integer",
		"status":    "integer",
		"eventType": "integer",
		"payload":   "[]byte",
		//"aggregates": "[]string", // is this to be used by the filter ?
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
