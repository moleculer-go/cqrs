package cqrs

import (
	"github.com/moleculer-go/moleculer"
	"github.com/moleculer-go/moleculer/broker"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("CQRS Pluggin", func() {

	It("should store event", func() {
		eventStore := Store("property")
		started := make(chan bool, 1)
		service := moleculer.ServiceSchema{
			Name:   "property",
			Mixins: []moleculer.Mixin{eventStore.Mixin()},
			Actions: []moleculer.Action{
				{
					Name:    "create",
					Handler: eventStore.Save("property.created"),
				},
			},
			Started: func(moleculer.BrokerContext, moleculer.ServiceSchema) {
				started <- true
			},
		}
		bkr := broker.New()
		bkr.Publish(service)
		bkr.Start()
		<-started

		r := <-bkr.Call("property.create", map[string]string{
			"listingId":    "100000",
			"externalId":   "123-abc",
			"status":       "ENABLED",
			"propertyName": "Clarksville Cottage",
			"productType":  "PAY_PER_BOOKING_V2",
			"sourceSite":   "HOMEAWAY_US",
		})
		Expect(r.Error()).Should(Succeed())

	})

})
