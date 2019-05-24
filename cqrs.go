package cqrs

import (
	"strings"
	"time"

	"github.com/moleculer-go/moleculer"
	"github.com/moleculer-go/moleculer/payload"
	"github.com/moleculer-go/moleculer/serializer"
	log "github.com/sirupsen/logrus"
)

const (
	StatusCreated    = 0
	StatusProcessing = 1
	StatusComplete   = 2
	StatusFailed     = 3
	StatusRetrying   = 4
)

type EventStore struct {
	name         string
	storeService moleculer.ServiceSchema

	brokerContext moleculer.BrokerContext
	serviceSchema moleculer.ServiceSchema
	serializer    serializer.Serializer
	logger        *log.Entry
}

func Store(name string) *EventStore {
	return &EventStore{
		name: name,
	}
}

// serviceStarted host service started.
func (e *EventStore) serviceStarted(c moleculer.BrokerContext, svc moleculer.ServiceSchema) {
	e.brokerContext = c
	e.serviceSchema = svc
	e.logger = c.Logger().WithField("cqrs", e.name)
	e.serializer = serializer.CreateJSONSerializer(e.logger)
	e.createService()
	c.Publish(e.storeService)
	c.WaitFor(e.storeService.Name)
}

func (e *EventStore) settings() map[string]string {
	setts, ok := e.serviceSchema.Settings["cqrs"]
	if ok {
		sm, ok := setts.(map[string]string)
		if ok {
			return sm
		}
	}
	return map[string]string{}
}

// resolveSQLiteURI check if is memory of file based db.
func (e *EventStore) resolveSQLiteURI() string {
	s := e.settings()
	folder := s["sqliteFolder"]
	if folder == "" || folder == "memory" {
		return "file:memory:?mode=memory"
	}
	return "file://" + folder
}

func (e *EventStore) createService() {
	e.storeService = createStoreService(e.name+"_store", e.settings()["extraFields"], e.resolveSQLiteURI())
}

// parseExtraParams extract valid extra parameters
func (e *EventStore) parseExtraParams(params []map[string]interface{}) map[string]interface{} {
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

// validExtras check if all fields in the extra map are valid
// (i.e. exists in the settings cqrs.extraFields )
func (e *EventStore) validExtras(extra map[string]interface{}) bool {
	extraFields, hasExtraFields := e.settings()["extraFields"]
	if !hasExtraFields || len(extra) == 0 {
		return false
	}
	for key, _ := range extra {
		if strings.Index(extraFields, key) == -1 {
			return false
		}
	}
	return true
}

// Save create an action handler that saves the action's payload as an event.
// extraParams are label=value to be saved in the event record.
// if it fails to save the event to the store it emits the event eventName.failed
func (e *EventStore) Save(eventName string, extraParams ...map[string]interface{}) moleculer.ActionHandler {
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
		r := <-c.Call(e.storeService.Name+".create", event)
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

// Mixin return the mixin schema for CQRS plugin
func (e *EventStore) Mixin() moleculer.Mixin {
	return moleculer.Mixin{
		Name:    "cqrs-mixin",
		Started: e.serviceStarted,
	}
}
