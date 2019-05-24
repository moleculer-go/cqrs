package cqrs

import (
	"github.com/moleculer-go/moleculer/serializer"

	"github.com/moleculer-go/moleculer"
	"github.com/moleculer-go/moleculer/broker"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("CQRS Pluggin", func() {
	logLevel := "debug"
	var bkr *broker.ServiceBroker
	createBroker := func() *broker.ServiceBroker {
		eventStore := Store("property")
		service := moleculer.ServiceSchema{
			Name:   "property",
			Mixins: []moleculer.Mixin{eventStore.Mixin()},
			Actions: []moleculer.Action{
				{
					Name:    "create",
					Handler: eventStore.Save("property.created"),
				},
			},
		}
		bkr := broker.New(&moleculer.Config{
			LogLevel: logLevel,
		})
		bkr.Publish(service)
		return bkr
	}

	BeforeEach(func() {
		bkr = createBroker()
		bkr.Start()
	})

	AfterEach(func() {
		bkr.Stop()
	})

	It("should store an event", func() {
		json := serializer.CreateJSONSerializer(bkr.GetLogger("", ""))
		r := <-bkr.Call("property.create", map[string]string{
			"listingId":    "100000",
			"externalId":   "123-abc",
			"status":       "ENABLED",
			"propertyName": "Clarksville Cottage",
			"productType":  "PAY_PER_BOOKING_V2",
			"sourceSite":   "HOMEAWAY_US",
		})
		Expect(r.Error()).Should(Succeed())
		Expect(r.Get("created").Exists()).Should(BeTrue())
		Expect(r.Get("event").String()).Should(Equal("property.created"))
		Expect(r.Get("id").Int()).Should(Equal(1))
		Expect(r.Get("status").Int()).Should(Equal(0))
		bts := r.Get("payload").ByteArray()
		p := json.BytesToPayload(&bts)
		Expect(p.Get("listingId").String()).Should(Equal("100000"))
		Expect(p.Get("externalId").String()).Should(Equal("123-abc"))
		Expect(p.Get("status").String()).Should(Equal("ENABLED"))
		Expect(p.Get("propertyName").String()).Should(Equal("Clarksville Cottage"))
		Expect(p.Get("productType").String()).Should(Equal("PAY_PER_BOOKING_V2"))
		Expect(p.Get("sourceSite").String()).Should(Equal("HOMEAWAY_US"))
	})

	It("should process incoming events", func(done Done) {
		propertyCreated := make(chan moleculer.Payload, 1)
		bkr.Publish(moleculer.ServiceSchema{
			Name: "test_incoming_events",
			Events: []moleculer.Event{
				{
					Name: "property.created",
					Handler: func(c moleculer.Context, p moleculer.Payload) {
						propertyCreated <- p
					},
				},
			},
		})

		r := <-bkr.Call("property.create", map[string]string{
			"listingId":    "100000",
			"externalId":   "123-abc",
			"status":       "ENABLED",
			"propertyName": "Clarksville Cottage",
			"productType":  "PAY_PER_BOOKING_V2",
			"sourceSite":   "HOMEAWAY_US",
		})
		Expect(r.Error()).Should(Succeed())

		evt := <-propertyCreated
		Expect(evt.Error()).Should(Succeed())
		Expect(evt.Get("listingId")).Should(Equal("100000"))

		close(done)
	})

})
