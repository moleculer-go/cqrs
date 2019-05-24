package property

import (
	"github.com/moleculer-go/cqrs"
	"github.com/moleculer-go/moleculer"
)

var eventStore = cqrs.Store("property")

var service = moleculer.ServiceSchema{
	Name:   "property",
	Mixins: []moleculer.Mixin{eventStore.Mixin()},
	Actions: []moleculer.Action{
		{
			Name:    "create",
			Handler: eventStore.Save("property.created"),
		},
	},
	Events: []moleculer.Event{
		{
			//event fired by the cqrs mixin read pump.
			//it keeps checking for new events and when there
			//are any, it emits this event.
			Name:    "property.created.saved",
			Handler: propertyCreated,
		},
	},
}

// propertyCreated process property created event
// save the new property on the right aggregate and emit
// property.created.successfully at the end of the process or
// property.created.error when there is an issue/error trying to create the property
func propertyCreated(context moleculer.Context, property moleculer.Payload) {

	//save record on aggregate

	context.Emit("property.created.successfully", property)
}
