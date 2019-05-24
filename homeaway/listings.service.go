package homeaway

import (
	"github.com/moleculer-go/moleculer"
)

var service = moleculer.ServiceSchema{
	Name: "homeaway.listings",
	Actions: []moleculer.Action{
		{
			Name:    "fetchListings",
			Handler: fetchListings,
		},
	},
	Events: []moleculer.Event{
		{
			Name:    "homeway.listing.received",
			Handler: listingReceived,
		},
	},
}

// fetchListings fetch listings from the given homeway account,
// for new listings emit the event homeway.listing.received for each
// listing returned.
func fetchListings(context moleculer.Context, params moleculer.Payload) interface{} {

	return nil
}

// listingReceived a listing from homeaway was received.
// check if the listing is new or if needs to be updated.
// for new listings call property.created and for updates
// call property.updated
func listingReceived(context moleculer.Context, params moleculer.Payload) {

	return nil
}
