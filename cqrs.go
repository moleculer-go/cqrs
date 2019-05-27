package cqrs

import (
	"github.com/moleculer-go/moleculer"
	"github.com/moleculer-go/store"
)

type AdapterFactory func(name string, fields, settings map[string]interface{}) store.Adapter

type Transformer func(context moleculer.Context, params moleculer.Payload) moleculer.Payload
type ManyTransformer func(context moleculer.Context, params moleculer.Payload) []moleculer.Payload

type EventStorer interface {
	Mixin() moleculer.Mixin
	NewEvent(eventName string, extraParams ...map[string]interface{}) moleculer.ActionHandler
}

type Aggregator interface {
	Mixin() moleculer.Mixin
	// Create creates an aggregate record. Uses transformer to transfor the event into the aggregate record.
	// emits:
	// property.created.successfully at the end of the process or
	// property.created.error when there is an issue/error transformring the event
	Create(Transformer) moleculer.EventHandler
	CreateMany(ManyTransformer) moleculer.EventHandler

	//ideas
	//UpdateMany
	//Update
	//Remove
}
