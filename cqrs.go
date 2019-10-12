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
	// StartSnapshot(snapshotName string, aggregateMetadata map[string]interface{}) error
	// CompleteSnapshot(snapshotName string) error
	// FailSnapshot(snapshotName string) error

	// PauseEvents() error
	// StartEvents()

	Name() string
}

//BackupStrategy function that backups the aggregate data
type BackupStrategy func(snapshotID string) error

//RestoreStrategy function that restores the aggregate data based on the snapshotID
type RestoreStrategy func(snapshotID string) error

//SnapshotStrategy factory function to create a snapshot strategy.
type SnapshotStrategy func(aggregateName string) (BackupStrategy, RestoreStrategy)

//type AggregateRestore func(aggregateName string) RestoreStrategy

type StoreFactory func(name string, cqrsFields, settings map[string]interface{}) store.Adapter

type EventMapping interface {
	//From(eventName string) moleculer.Event

	// Create 1 (one) aggregate record with the result (single payload) of the transformations.
	Create(transformAction string) moleculer.Event

	// Create multiple aggregate records with the result (list of payloads) of the transformations.
	CreateMany(transformAction string) moleculer.Event

	// Update an existing aggregate record if the result of the transformation has an id field,
	// or creates a new aggregate record if no id is present.
	Update(transformAction string) moleculer.Event
}

type Aggregator interface {
	Mixin() moleculer.Mixin

	// Snapshot configure the snapshot behaviour of the aggregate
	Snapshot(eventStore string) Aggregator

	//On starts a event mapping chain. It takes the event name and returns an
	// EventMapping object, which is used to map the transformation actions and these methods
	// return an moleculer.Event object.
	// Example:
	// On("user.created").Create("profile.userProfile")  -> returns moleculer.Event :)
	// this will listen for the event: user.created and it will use
	// the action profile.userProfile to transform the user.created event payload,
	// the result will used to create a single new record in the aggregate.
	On(event string) EventMapping
}

type M map[string]interface{}
