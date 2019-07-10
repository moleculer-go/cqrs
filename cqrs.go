package cqrs

import (
	"github.com/moleculer-go/moleculer"
	"github.com/moleculer-go/store"
)

type StoreFactory interface {
	Backup(name string) error
	Create(name string, fields, settings map[string]interface{}) store.Adapter
}

type Transformer func(context moleculer.Context, params moleculer.Payload) moleculer.Payload
type ManyTransformer func(context moleculer.Context, params moleculer.Payload) []moleculer.Payload

type EventStorer interface {
	Mixin() moleculer.Mixin
	PersistEvent(eventName string, extraParams ...map[string]interface{}) moleculer.ActionHandler
	StartSnapshot(snapshotName string, aggregateMetadata map[string]interface{}) error
	CompleteSnapshot(snapshotName string) error
}

type SnapshotSetup interface {
	Backup(string) error
}

type Aggregator interface {
	Mixin() moleculer.Mixin
	// Create creates an aggregate record. Uses transformer to transfor the event into the aggregate record.
	// emits:
	// property.created.successfully at the end of the process or
	// property.created.error when there is an issue/error transformring the event
	Create(Transformer) moleculer.EventHandler
	CreateMany(ManyTransformer) moleculer.EventHandler
	// Update changes an existing aggregate record
	// if the result of the transformation has an id,
	// or creates a new aggreagate record if no id is present.
	Update(Transformer) moleculer.EventHandler

	// Snapshot configure the snapshot behaviour of the aggregate
	Snapshot(EventStorer) Aggregator

	//Remove
}
