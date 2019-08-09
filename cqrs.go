package cqrs

import (
	"github.com/moleculer-go/moleculer"
	"github.com/moleculer-go/store"
)

type Transformer func(context moleculer.Context, params moleculer.Payload) moleculer.Payload
type ManyTransformer func(context moleculer.Context, params moleculer.Payload) []moleculer.Payload

type EventStorer interface {
	Mixin() moleculer.Mixin
	PersistEvent(eventName string, extraParams ...map[string]interface{}) moleculer.ActionHandler

	//try to move these to a interface just around snapshoter
	StartSnapshot(snapshotName string, aggregateMetadata map[string]interface{}) error
	CompleteSnapshot(snapshotName string) error
	FailSnapshot(snapshotName string) error
}

type BackupStrategy func(snapshotID string, settings map[string]interface{}) error
type StoreFactory func(name string, cqrsFields, settings map[string]interface{}) store.Adapter

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
