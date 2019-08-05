package cqrs

import (
	"strconv"
	"time"
	"errors"
	"fmt"
	"os"

	"github.com/moleculer-go/moleculer/payload"
	"github.com/moleculer-go/moleculer/serializer"
	"github.com/moleculer-go/store"
	"github.com/moleculer-go/store/sqlite"

	"github.com/moleculer-go/moleculer"
	"github.com/moleculer-go/moleculer/broker"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

type storeFactoryMock struct {
	backup func(name string) (err error)
	create func(name string, cqrsFields, settings map[string]interface{}) store.Adapter
}

func (f *storeFactoryMock) Backup(name string) (err error) {
	if f.backup == nil {
		return errors.New("backup not setup")
	}
	return f.backup(name)
}

func (f *storeFactoryMock) Create(name string, cqrsFields, settings map[string]interface{}) store.Adapter {
	return f.create(name, cqrsFields, settings)
}

func sqliteMemory(fields ... map[string]interface{}) (func (name string, cqrsFields, settings map[string]interface{}) store.Adapter) {
	return func (name string, cqrsFields, settings map[string]interface{}) store.Adapter { 
		allFields := append(fields, cqrsFields)
		return &sqlite.Adapter{
			URI:     "file:memory:?mode=memory",
			Table:   name,
			Columns: FieldsToSQLiteColumns(allFields...),
		}
	}
}

func sqliteBackup(dbFolder, backupFolder string) func(string) error {
	return func(name string) error {
		return sqlite.FileCopyBackup(name, "file:" + dbFolder, backupFolder)
	}
}

func sqliteFile(folder string, fields ... map[string]interface{}) (func (name string, cqrsFields, settings map[string]interface{}) store.Adapter) {
	return func (name string, cqrsFields, settings map[string]interface{}) store.Adapter { 
		allFields := append(fields, cqrsFields)
		//make sure folder exists
		err := os.MkdirAll(folder, os.ModePerm)
		if err != nil {
			fmt.Println("### Error trying to create sqlite db folder: ", folder, " Error: ", err)
		}
		fmt.Println("### SQLIte Folder created ! --> ", folder)
		return &sqlite.Adapter{
			URI:     "file:" + folder,
			Table:   name,
			Columns: FieldsToSQLiteColumns(allFields...),
		}
	}
}

var _ = Describe("CQRS Pluggin", func() {
	logLevel := "error"

	Describe("Event Store", func() {

		createBroker := func(dispatchBatchSize int) *broker.ServiceBroker {
			eventStore := EventStore("propertyEventStore", &storeFactoryMock{create:sqliteMemory()})
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
						Handler: eventStore.PersistEvent("property.created"),
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
					"listingId":    "some_id_" + strconv.Itoa(index),
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
				Expect(evt.Error()).Should(Succeed())
				Expect(evt.Get("event").String()).Should(Equal("property.created"))
				list[index] = evt
			}
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
		}, 4)

		It("should store extra fields in the event", func(done Done) {
			eventStore := EventStore("userEventStore", &storeFactoryMock{create:sqliteMemory()}, map[string]interface{}{
				"tag": "string",
			})
			service := moleculer.ServiceSchema{
				Name:   "user",
				Mixins: []moleculer.Mixin{eventStore.Mixin()},
				Actions: []moleculer.Action{
					{
						Name:    "create",
						Handler: eventStore.PersistEvent("user.created", M{"tag": "valueX"}),
					},
				},
			}
			bkr := broker.New(&moleculer.Config{
				LogLevel: logLevel,
			})
			bkr.Publish(service)
			bkr.Start()
			r := <-bkr.Call("user.create", map[string]string{
				"userName": "johnTravolta",
			})
			Expect(r.Error()).Should(Succeed())
			Expect(r.Get("created").Exists()).Should(BeTrue())
			Expect(r.Get("event").String()).Should(Equal("user.created"))
			Expect(r.Get("tag").String()).Should(Equal("valueX"))
			Expect(r.Get("id").Int()).Should(Equal(1))
			bkr.Stop()
			close(done)
		}, 4)

	})

	Describe("Aggregate", func() {
		createBrokerWithEventStore := func(dispatchBatchSize int, eventStore EventStorer) *broker.ServiceBroker {
			service := moleculer.ServiceSchema{
				Name:   "property",
				Mixins: []moleculer.Mixin{eventStore.Mixin()},
				Actions: []moleculer.Action{
					{
						Name:    "create",
						Handler: eventStore.PersistEvent("property.created"),
					},
				},
			}
			bkr := broker.New(&moleculer.Config{
				LogLevel: logLevel,
			})
			bkr.Publish(service)
			return bkr 
		}
		createBroker := func(dispatchBatchSize int) (*broker.ServiceBroker, EventStorer) {
			eventStore := EventStore("propertyEventStore", &storeFactoryMock{create:sqliteMemory()})
			return createBrokerWithEventStore(
				dispatchBatchSize, 
				eventStore,
			), eventStore
		}

		notifications := Aggregate("notificationsAggregate", &storeFactoryMock{create:sqliteMemory(map[string]interface{}{
			"eventId":      "integer",
			"smsContent":   "string",
			"pushContent":  "string",
			"emailContent": "string",
		} )})

		//transform the incoming property.created event into X property notification records
		createNotificationsService := func(records int, notificationsCreatedChan chan []moleculer.Payload) moleculer.ServiceSchema {
			//createNotifications transforms the 'property.created' event and
			createNotifications := func(context moleculer.Context, event moleculer.Payload) []moleculer.Payload {
				property := event.Get("payload")
				name := "John"
				mobileMsg := "Hi " + name + ", Property " + property.Get("name").String() + " with " + property.Get("bedrooms").String() + " was added to your account!"
				notifications := []moleculer.Payload{}
				for index := 0; index < 5; index++ {
					notification := payload.New(M{
						"eventId":      event.Get("id").Int(),
						"smsContent":   "[" + strconv.Itoa(index) + "] " + mobileMsg,
						"pushContent":  "[" + strconv.Itoa(index) + "] " + mobileMsg,
						"emailContent": "...",
					})
					notifications = append(notifications, notification)
				}
				//Just for testing purposes.. so we can use this channel to check if notifications were generated
				go func() { notificationsCreatedChan <- notifications }()
				return notifications
			}
			return moleculer.ServiceSchema{
				Name:   "propertyNotifier",
				Mixins: []moleculer.Mixin{notifications.Mixin()},
				Events: []moleculer.Event{
					{
						Name:    "property.created",
						Handler: notifications.CreateMany(createNotifications),
					},
				},
			}
		}

		It("should transform one event and save one aggregate record", func(done Done) {
			bkr, _ := createBroker(1)
			notificationCreated := make(chan moleculer.Payload, 1)
			//transform the incoming property.created event into a property notification aggregate record.
			transformPropertyCreated := func(context moleculer.Context, event moleculer.Payload) moleculer.Payload {
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

			//wait until one record was created in the aggregate :)
			for {
				notificationsCount = <-bkr.Call("notificationsAggregate.count", M{})
				Expect(notificationsCount.Error()).Should(Succeed())
				if notificationsCount.Int() == 1 {
					break
				}
			}

			bkr.Stop()
			close(done)
		}, 3)

		It("should transform one event and save 5 aggregate records", func(done Done) {
			bkr, _ := createBroker(1)
			notificationsCreated := make(chan []moleculer.Payload, 1)
			bkr.Publish(createNotificationsService(5, notificationsCreated))
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
			notifications := <-notificationsCreated
			Expect(notifications[0].Get("eventId").Int()).Should(Equal(evt.Get("id").Int()))
			Expect(notifications[0].Get("smsContent").String()).Should(Equal("[0] Hi John, Property Beach villa with 12 was added to your account!"))
			Expect(notifications[0].Get("pushContent").String()).Should(Equal("[0] Hi John, Property Beach villa with 12 was added to your account!"))
			Expect(notifications[0].Get("emailContent").String()).Should(Equal("..."))
			Expect(notifications[3].Get("pushContent").String()).Should(Equal("[3] Hi John, Property Beach villa with 12 was added to your account!"))

			//wait for one record to be created in the aggregate :)
			for {
				notificationsCount = <-bkr.Call("notificationsAggregate.count", M{})
				Expect(notificationsCount.Error()).Should(Succeed())
				if notificationsCount.Int() == 5 {
					break
				}
			}

			bkr.Stop()
			close(done)
		}, 3)

		It("should fail snapshot when aggregate is not configured for snapsot :)", func(done Done) {
			bkr, _ := createBroker(1)
			notificationsCreated := make(chan []moleculer.Payload, 1)
			bkr.Publish(createNotificationsService(5, notificationsCreated))
			bkr.Start()
			snapshotName := <-bkr.Call("notificationsAggregate.snapshot", M{})
			Expect(snapshotName.Error()).Should(HaveOccurred())
			Expect(snapshotName.Error().Error()).Should(Equal("snapshot not configured for this aggregate. eventStore is nil"))
			bkr.Stop()
			close(done)
		})

		It("should fail snapshot when aggregate backup is not configured - missing backupFolder from service settings", func(done Done) {
			bkr, eventStore := createBroker(1)
			notificationsCreated := make(chan []moleculer.Payload, 1)
			notifications.Snapshot(eventStore)
			bkr.Publish(createNotificationsService(5, notificationsCreated))
			bkr.Start()

			//aggregate starts empty
			notificationsCount := <-bkr.Call("notificationsAggregate.count", M{})
			Expect(notificationsCount.Error()).Should(Succeed())
			Expect(notificationsCount.Int()).Should(Equal(0))

			//create 10 properties..
			for index := 0; index < 10; index++ {
				evt := <-bkr.Call("property.create", map[string]string{
					"listingId": "100000",
					"name":      "Beach villa",
					"bedrooms":  "12",
				})
				Expect(evt.Error()).Should(Succeed())
			}

			//wait for 50 records in the aggregate :)
			for {
				go func() { <-notificationsCreated }()
				notificationsCount = <-bkr.Call("notificationsAggregate.count", M{})
				Expect(notificationsCount.Error()).Should(Succeed())
				if notificationsCount.Int() == 50 {
					break
				}
			}

			result := <-bkr.Call("notificationsAggregate.snapshot", M{})
			Expect(result.Error()).ShouldNot(Succeed())
			Expect(result.Error().Error()).Should(Equal("aggregate.snapshot action failed. We could not backup the aggregate. Error: backup not setup"))

			bkr.Stop()
			close(done)
		}, 2)

		It("should snapshot the current aggregate data - backup data and create event to record snapshot", func(done Done) {
			dbFolder := "/Users/rafael/temp_test_dbs/snapshot"
			bkpFolder := "/Users/rafael/temp_test_dbs/snapshot_bkp"
			eventStore := EventStore("propertyEventStore", &storeFactoryMock{create:sqliteFile(dbFolder), backup:sqliteBackup(dbFolder, bkpFolder)})
			bkr := createBrokerWithEventStore(1, eventStore)
			notificationsCreated := make(chan []moleculer.Payload, 1)
			notifications.Snapshot(eventStore)
			service := createNotificationsService(5, notificationsCreated)
			// service.Settings = M{
			// 	"backupFolder": "~/snapshot_bkp/",
			// }
			bkr.Publish(service)
			bkr.Start()

			//aggregate starts empty
			notificationsCount := <-bkr.Call("notificationsAggregate.count", M{})
			Expect(notificationsCount.Error()).Should(Succeed())
			Expect(notificationsCount.Int()).Should(Equal(0))

			//create 10 properties..
			for index := 0; index < 10; index++ {
				evt := <-bkr.Call("property.create", map[string]string{
					"listingId": "100000",
					"name":      "Beach villa",
					"bedrooms":  "12",
				})
				Expect(evt.Error()).Should(Succeed())
			}

			//wait for 50 records in the aggregate :)
			for {
				go func() { <-notificationsCreated }()
				notificationsCount = <-bkr.Call("notificationsAggregate.count", M{})
				Expect(notificationsCount.Error()).Should(Succeed())
				if notificationsCount.Int() == 50 {
					break
				}
			}

			snapshotName := <-bkr.Call("notificationsAggregate.snapshot", M{})
			Expect(snapshotName.Error()).Should(Succeed())
			Expect(snapshotName.String()).ShouldNot(Equal(""))

			//check snapshot event

			//check bkp file created

			//check event pump is back

			bkr.Stop()
			close(done)
		}, 10)

	})

})
