package cqrs

import (
	"fmt"
	"time"

	"github.com/moleculer-go/moleculer/payload"
	"github.com/moleculer-go/moleculer/serializer"
	"github.com/moleculer-go/store"
	"github.com/moleculer-go/store/sqlite"

	"github.com/moleculer-go/moleculer"
	"github.com/moleculer-go/moleculer/broker"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("CQRS Pluggin", func() {
	logLevel := "debug"

	adapterFactory := func(fields map[string]interface{}) AdapterFactory {
		return func(name string, cqrsFields, settings map[string]interface{}) store.Adapter {
			return &sqlite.Adapter{
				URI:     "file:memory:?mode=memory",
				Table:   name,
				Columns: FieldsToSQLiteColumns(fields, cqrsFields),
			}
		}
	}

	Describe("Event Store", func() {

		createBroker := func(dispatchBatchSize int) *broker.ServiceBroker {
			eventStore := EventStore("property", adapterFactory(map[string]interface{}{}))
			service := moleculer.ServiceSchema{
				Name:   "property",
				Mixins: []moleculer.Mixin{eventStore.Mixin()},
				Settings: M{
					"eventStore": M{
						"dispatchBatchSize": dispatchBatchSize,
						"poolingInterval":   2 * time.Microsecond, // :)
					},
				},
				Actions: []moleculer.Action{
					{
						Name:    "create",
						Handler: eventStore.NewEvent("property.created"),
					},
				},
			}
			bkr := broker.New(&moleculer.Config{
				LogLevel: logLevel,
			})
			bkr.Publish(service)
			return bkr
		}

		It("should store an event", func() {
			bkr := createBroker(1)
			json := serializer.CreateJSONSerializer(bkr.GetLogger("", ""))
			bkr.Start()
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
			bkr.Stop()
		})

		It("should dispatch stored events", func(done Done) {
			dispatchBatchSize := 5
			bkr := createBroker(dispatchBatchSize)
			propertyCreated := make(chan moleculer.Payload, 1)
			bkr.Publish(moleculer.ServiceSchema{
				Name: "test_incoming_events",
				Events: []moleculer.Event{
					{
						Name: "property.created",
						Handler: func(c moleculer.Context, p moleculer.Payload) {
							fmt.Println("Event called -> property.created !")
							propertyCreated <- p
						},
					},
				},
			})
			bkr.Start()

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
			Expect(evt.Get("event").String()).Should(Equal("property.created"))

			evtPayload := evt.Get("payload")
			Expect(evtPayload.Get("listingId").String()).Should(Equal("100000"))
			Expect(evtPayload.Get("externalId").String()).Should(Equal("123-abc"))
			Expect(evtPayload.Get("status").String()).Should(Equal("ENABLED"))

			//check the dispatch in batch
			<-bkr.Call("property.$stopDispatch", nil)
			for index := 0; index < dispatchBatchSize; index++ {
				r := <-bkr.Call("property.create", map[string]string{
					"listingId":    "some_id_" + string(index),
					"externalId":   "123-abc",
					"status":       "ENABLED",
					"propertyName": "Clarksville Cottage",
					"productType":  "PAY_PER_BOOKING_V2",
					"sourceSite":   "HOMEAWAY_US",
				})
				Expect(r.Error()).Should(Succeed())
			}

			//start dispatching events again
			<-bkr.Call("property.$startDispatch", nil)
			list := make([]moleculer.Payload, dispatchBatchSize)
			for index := 0; index < dispatchBatchSize; index++ {
				evt := <-propertyCreated
				fmt.Println("Event received - (index: ", index, ") event: ", evt)

				Expect(evt.Error()).Should(Succeed())
				Expect(evt.Get("event").String()).Should(Equal("property.created"))
				list[index] = evt
			}
			fmt.Println("All events received!")
			Expect(list[0].Get("batchSize").Int()).Should(Equal(dispatchBatchSize))
			Expect(list[0].Get("payload").Get("externalId").String()).Should(Equal("123-abc"))
			Expect(list[0].Get("payload").Get("status").String()).Should(Equal("ENABLED"))

			Expect(list[1].Get("batchSize").Int()).Should(Equal(dispatchBatchSize))
			Expect(list[2].Get("batchSize").Int()).Should(Equal(dispatchBatchSize))
			Expect(list[3].Get("batchSize").Int()).Should(Equal(dispatchBatchSize))
			Expect(list[4].Get("batchSize").Int()).Should(Equal(dispatchBatchSize))
			Expect(list[4].Get("payload").Get("externalId").String()).Should(Equal("123-abc"))
			Expect(list[4].Get("payload").Get("status").String()).Should(Equal("ENABLED"))

			<-bkr.Call("property.$stopDispatch", nil)

			bkr.Stop()
			close(done)
		}, 10)

	})

	Describe("Aggregate", func() {

		createBroker := func(dispatchBatchSize int) *broker.ServiceBroker {
			eventStore := EventStore("property", adapterFactory(map[string]interface{}{}))
			service := moleculer.ServiceSchema{
				Name:   "property",
				Mixins: []moleculer.Mixin{eventStore.Mixin()},
				Actions: []moleculer.Action{
					{
						Name:    "create",
						Handler: eventStore.NewEvent("property.created"),
					},
				},
			}
			bkr := broker.New(&moleculer.Config{
				LogLevel: logLevel,
			})
			bkr.Publish(service)
			return bkr
		}

		notifications := Aggregate("notifications", adapterFactory(map[string]interface{}{
			"eventId":      "integer",
			"smsContent":   "string",
			"pushContent":  "string",
			"emailContent": "string",
		}))

		It("should transform event and save one aggregate record", func(done Done) {
			bkr := createBroker(1)
			notificationCreated := make(chan moleculer.Payload, 1)
			//transform the incoming property.created event into a property notification aggregate record.
			transformPropertyCreated := func(context moleculer.Context, event moleculer.Payload) moleculer.Payload {
				fmt.Println("Event called -> property.created ! event: ", event)
				property := event.Get("payload")
				name := "John"
				mobileMsg := "Hi " + name + ", Property " + property.Get("name").String() + " with " + property.Get("bedrooms").String() + " was added to your account!"
				notification := payload.New(M{
					"eventId":      event.Get("id").Int(),
					"smsContent":   mobileMsg,
					"pushContent":  mobileMsg,
					"emailContent": "...",
				})
				notificationCreated <- notification
				return notification
			}
			bkr.Publish(moleculer.ServiceSchema{
				Name:   "propertyNotifier",
				Mixins: []moleculer.Mixin{notifications.Mixin()},
				Events: []moleculer.Event{
					{
						Name:    "property.created",
						Handler: notifications.Create(transformPropertyCreated),
					},
				},
			})
			bkr.Start()

			//aggregate starts empty
			notificationsCount := <-bkr.Call("notificationsAggregate.count", M{})
			Expect(notificationsCount.Error()).Should(Succeed())
			Expect(notificationsCount.Int()).Should(Equal(0))

			evt := <-bkr.Call("property.create", map[string]string{
				"listingId": "100000",
				"name":      "Beach villa",
				"bedrooms":  "12",
			})
			Expect(evt.Error()).Should(Succeed())
			notification := <-notificationCreated
			Expect(notification.Get("eventId").Int()).Should(Equal(evt.Get("id").Int()))
			Expect(notification.Get("smsContent").String()).Should(Equal("Hi John, Property Beach villa with 12 was added to your account!"))
			Expect(notification.Get("pushContent").String()).Should(Equal("Hi John, Property Beach villa with 12 was added to your account!"))
			Expect(notification.Get("emailContent").String()).Should(Equal("..."))

			//check if one record was created in the aggregate :)
			notificationsCount = <-bkr.Call("notificationsAggregate.count", M{})
			Expect(notificationsCount.Error()).Should(Succeed())
			Expect(notificationsCount.Int()).Should(Equal(1))

			bkr.Stop()
			close(done)
		}, 3)

	})

})
