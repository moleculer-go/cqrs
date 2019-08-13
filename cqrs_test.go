package cqrs

import (
	"fmt"
	"os"
	"strconv"
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

// type storeFactoryMock struct {
// 	backup func(name string) (err error)
// 	create func(name string, cqrsFields, settings map[string]interface{}) store.Adapter
// }

// func (f *storeFactoryMock) Backup(name string) (err error) {
// 	if f.backup == nil {
// 		return errors.New("backup not setup")
// 	}
// 	return f.backup(name)
// }

// func (f *storeFactoryMock) Create(name string, cqrsFields, settings map[string]interface{}) store.Adapter {
// 	return f.create(name, cqrsFields, settings)
// }

func sqliteMemory(fields ...map[string]interface{}) func(name string, cqrsFields, settings map[string]interface{}) store.Adapter {
	return func(name string, cqrsFields, settings map[string]interface{}) store.Adapter {
		allFields := append(fields, cqrsFields)
		return &sqlite.Adapter{
			URI:     "file:memory:?mode=memory",
			Table:   name,
			Columns: FieldsToSQLiteColumns(allFields...),
		}
	}
}

// func sqliteBackup(dbfile, backupFolder string) func(string) error {
// 	return func(name string) error {
// 		return sqlite.FileCopyBackup(name, "file:"+dbfile, backupFolder)
// 	}
// }

func sqliteFile(baseFolder string, fields ...map[string]interface{}) func(name string, cqrsFields, settings map[string]interface{}) store.Adapter {
	return func(name string, cqrsFields, settings map[string]interface{}) store.Adapter {
		allFields := append(fields, cqrsFields)
		//make sure folder exists
		folder := baseFolder + "/" + name
		err := os.MkdirAll(folder, os.ModePerm)
		if err != nil {
			fmt.Println("### Error trying to create sqlite db folder: ", folder, " Error: ", err)
		}
		//fmt.Println("### SQLite Folder created ! --> ", folder)
		return &sqlite.Adapter{
			URI:     "file:" + folder + "/store.db",
			Table:   name,
			Columns: FieldsToSQLiteColumns(allFields...),
		}
	}
}

var _ = Describe("CQRS Pluggin", func() {
	logLevel := "fatal"

	Describe("Event Store", func() {

		createBroker := func(dispatchBatchSize int) *broker.ServiceBroker {
			eventStore := EventStore(
				"propertyEventStore",
				sqliteMemory())
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
			Expect(r.Error()).Should(BeNil())
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
			Expect(r.Error()).Should(BeNil())
			evt := <-propertyCreated
			Expect(evt.Error()).Should(BeNil())
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
				Expect(r.Error()).Should(BeNil())
			}

			//start dispatching events again
			<-bkr.Call("property.$startDispatch", nil)
			list := make([]moleculer.Payload, dispatchBatchSize)
			for index := 0; index < dispatchBatchSize; index++ {
				evt := <-propertyCreated
				Expect(evt.Error()).Should(BeNil())
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
			eventStore := EventStore(
				"userEventStore",
				sqliteMemory(),
				map[string]interface{}{
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
			Expect(r.Error()).Should(BeNil())
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
			eventStore := EventStore("propertyEventStore", sqliteMemory())
			return createBrokerWithEventStore(
				dispatchBatchSize,
				eventStore,
			), eventStore
		}

		notifications := Aggregate(
			"notificationsAggregate",
			sqliteMemory(map[string]interface{}{
				"eventId":      "integer",
				"smsContent":   "string",
				"pushContent":  "string",
				"emailContent": "string",
			}),
			NoSnapshot)

		//transform the incoming property.created event into X property notification records
		createNotificationsService := func(notifications Aggregator, records int, notificationsCreatedChan chan []moleculer.Payload) moleculer.ServiceSchema {
			//createNotifications transforms the 'property.created' event and
			createNotifications := func(context moleculer.Context, event moleculer.Payload) []moleculer.Payload {
				property := event.Get("payload")
				name := "John"
				mobileMsg := "Hi " + name + ", Property " + property.Get("name").String() + " with " + property.Get("bedrooms").String() + " was added to your account!"
				result := []moleculer.Payload{}
				for index := 0; index < 5; index++ {
					notification := payload.New(M{
						"eventId":      event.Get("id").Int(),
						"smsContent":   "[" + strconv.Itoa(index) + "] " + mobileMsg,
						"pushContent":  "[" + strconv.Itoa(index) + "] " + mobileMsg,
						"emailContent": "...",
					})
					result = append(result, notification)
				}
				//Just for testing purposes.. so we can use this channel to check if notifications were generated
				go func() { notificationsCreatedChan <- result }()
				return result
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
			Expect(notificationsCount.Error()).Should(BeNil())
			Expect(notificationsCount.Int()).Should(Equal(0))

			evt := <-bkr.Call("property.create", map[string]string{
				"listingId": "100000",
				"name":      "Beach villa",
				"bedrooms":  "12",
			})
			Expect(evt.Error()).Should(BeNil())
			notification := <-notificationCreated
			Expect(notification.Get("eventId").Int()).Should(Equal(evt.Get("id").Int()))
			Expect(notification.Get("smsContent").String()).Should(Equal("Hi John, Property Beach villa with 12 was added to your account!"))
			Expect(notification.Get("pushContent").String()).Should(Equal("Hi John, Property Beach villa with 12 was added to your account!"))
			Expect(notification.Get("emailContent").String()).Should(Equal("..."))

			//wait until one record was created in the aggregate :)
			for {
				notificationsCount = <-bkr.Call("notificationsAggregate.count", M{})
				Expect(notificationsCount.Error()).Should(BeNil())
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
			bkr.Publish(createNotificationsService(notifications, 5, notificationsCreated))
			bkr.Start()

			//aggregate starts empty
			notificationsCount := <-bkr.Call("notificationsAggregate.count", M{})
			Expect(notificationsCount.Error()).Should(BeNil())
			Expect(notificationsCount.Int()).Should(Equal(0))

			evt := <-bkr.Call("property.create", map[string]string{
				"listingId": "100000",
				"name":      "Beach villa",
				"bedrooms":  "12",
			})
			Expect(evt.Error()).Should(BeNil())
			notifications := <-notificationsCreated
			Expect(notifications[0].Get("eventId").Int()).Should(Equal(evt.Get("id").Int()))
			Expect(notifications[0].Get("smsContent").String()).Should(Equal("[0] Hi John, Property Beach villa with 12 was added to your account!"))
			Expect(notifications[0].Get("pushContent").String()).Should(Equal("[0] Hi John, Property Beach villa with 12 was added to your account!"))
			Expect(notifications[0].Get("emailContent").String()).Should(Equal("..."))
			Expect(notifications[3].Get("pushContent").String()).Should(Equal("[3] Hi John, Property Beach villa with 12 was added to your account!"))

			//wait for one record to be created in the aggregate :)
			for {
				notificationsCount = <-bkr.Call("notificationsAggregate.count", M{})
				Expect(notificationsCount.Error()).Should(BeNil())
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
			bkr.Publish(createNotificationsService(notifications, 5, notificationsCreated))
			bkr.Start()
			snapshotID := <-bkr.Call("notificationsAggregate.snapshot", M{})
			Expect(snapshotID.Error()).Should(HaveOccurred())
			Expect(snapshotID.Error().Error()).Should(Equal("snapshot not configured for this aggregate. eventStore is nil"))
			bkr.Stop()
			close(done)
		})

		It("should fail snapshot when aggregate backup is not configured - missing backupFolder from service settings", func(done Done) {
			bkr, eventStore := createBroker(1)
			notificationsCreated := make(chan []moleculer.Payload, 1)
			notifications.Snapshot(eventStore)
			bkr.Publish(createNotificationsService(notifications, 5, notificationsCreated))
			bkr.Start()

			//aggregate starts empty
			notificationsCount := <-bkr.Call("notificationsAggregate.count", M{})
			Expect(notificationsCount.Error()).Should(BeNil())
			Expect(notificationsCount.Int()).Should(Equal(0))

			//create 10 properties..
			for index := 0; index < 10; index++ {
				evt := <-bkr.Call("property.create", map[string]string{
					"listingId": "100000",
					"name":      "Beach villa",
					"bedrooms":  "12",
				})
				Expect(evt.Error()).Should(BeNil())
			}

			//wait for 50 records in the aggregate :)
			for {
				go func() { <-notificationsCreated }()
				notificationsCount = <-bkr.Call("notificationsAggregate.count", M{})
				Expect(notificationsCount.Error()).Should(BeNil())
				if notificationsCount.Int() == 50 {
					break
				}
			}

			result := <-bkr.Call("notificationsAggregate.snapshot", M{})
			fmt.Println(result)
			Expect(result.Error()).ShouldNot(BeNil())
			Expect(result.Error().Error()).Should(Equal("aggregate.snapshot action failed. We could not backup the aggregate. Error: no backup strategy"))

			bkr.Stop()
			close(done)
		}, 5)

		Context("File database with snapshots", func() {
			cacheFolder, _ := os.UserCacheDir()
			baseFolder := cacheFolder + "/cqrs_test_dbs"
			dbAggregatesFolder := baseFolder + "/aggregates"
			dbEventStoreFolder := baseFolder + "/eventStores"
			snapshotFolder := baseFolder + "/snapshots"

			BeforeEach(func() {
				err := os.RemoveAll(baseFolder)
				if err != nil {
					fmt.Println("** Error removing test database folder: ", baseFolder, " Error: ", err)
				}
			})

			//wait for x (size) records  from the count action
			waitForRecords := func(bkr *broker.ServiceBroker, action string, size int) {
				for {
					count := <-bkr.Call(action, M{})
					Expect(count.Error()).Should(BeNil())
					if count.Int() == size {
						break
					}
				}
			}

			populateAggregate := func(bkr *broker.ServiceBroker, notificationsCreated chan []moleculer.Payload) {
				//aggregate starts empty
				notificationsCount := <-bkr.Call("notificationsAggregate.count", M{})
				Expect(notificationsCount.Error()).Should(BeNil())
				Expect(notificationsCount.Int()).Should(Equal(0))

				//create 10 properties..
				for index := 0; index < 10; index++ {
					evt := <-bkr.Call("property.create", map[string]string{
						"listingId": "100000",
						"name":      "Beach villa",
						"bedrooms":  "12",
					})
					Expect(evt.Error()).Should(BeNil())
				}

				waitForRecords(bkr, "notificationsAggregate.count", 50)
			}

			waitForSnapShotEvent := func(bkr *broker.ServiceBroker) {
				for {
					snapEventCount := <-bkr.Call("propertyEventStore.count", M{"query": M{"eventType": TypeSnapshotCompleted}})
					Expect(snapEventCount.Error()).Should(BeNil())
					if snapEventCount.Int() == 1 {
						break
					}
				}
			}

			setup := func() *broker.ServiceBroker {
				eventStore := EventStore("propertyEventStore", sqliteFile(dbEventStoreFolder))
				notifications := Aggregate(
					"notificationsAggregate",

					sqliteFile(dbAggregatesFolder, map[string]interface{}{
						"eventId":      "integer",
						"smsContent":   "string",
						"pushContent":  "string",
						"emailContent": "string",
					}),
					FileCopyBackup(dbAggregatesFolder, snapshotFolder),
				)
				notifications.Snapshot(eventStore)
				notificationsCreated := make(chan []moleculer.Payload, 1)
				service := createNotificationsService(notifications, 5, notificationsCreated)
				bkr := createBrokerWithEventStore(1, eventStore)
				bkr.Publish(service)
				bkr.Start()
				populateAggregate(bkr, notificationsCreated)
				return bkr
			}

			It("should snapshot the current aggregate data - backup data and create event to record snapshot", func(done Done) {
				bkr := setup()

				snapshotID := <-bkr.Call("notificationsAggregate.snapshot", M{})
				Expect(snapshotID.Error()).Should(BeNil())
				Expect(snapshotID.String()).ShouldNot(Equal(""))

				waitForSnapShotEvent(bkr)

				//check snapshot folder was created
				Expect(snapshotFolder + "/" + snapshotID.String()).Should(BeADirectory())
				Expect(snapshotFolder + "/" + snapshotID.String() + "/notificationsAggregate").Should(BeADirectory())
				Expect(snapshotFolder + "/" + snapshotID.String() + "/notificationsAggregate/store.db").Should(BeAnExistingFile())
				Expect(snapshotFolder + "/" + snapshotID.String() + "/notificationsAggregate/store.db-shm").Should(BeAnExistingFile())
				Expect(snapshotFolder + "/" + snapshotID.String() + "/notificationsAggregate/store.db-wal").Should(BeAnExistingFile())

				//check event pump is back
				evt := <-bkr.Call("property.create", map[string]string{
					"listingId": "100000",
					"name":      "Beach villa",
					"bedrooms":  "12",
				})
				Expect(evt.Error()).Should(BeNil())

				//aggregate should have 55 records  :)
				waitForRecords(bkr, "notificationsAggregate.count", 55)

				bkr.Stop()
				close(done)
			}, 3)

			It("should restore a snapshot", func(done Done) {
				bkr := setup()

				snapshotID := <-bkr.Call("notificationsAggregate.snapshot", M{})
				Expect(snapshotID.Error()).Should(BeNil())
				Expect(snapshotID.String()).ShouldNot(Equal(""))

				waitForSnapShotEvent(bkr)

				//clean up all notificationsAggregate db
				bkr.Stop()
				Expect(os.RemoveAll(dbAggregatesFolder + "/notificationsAggregate")).Should(Succeed())
				bkr.Start()

				//aggregate should be empty
				notificationsCount := <-bkr.Call("notificationsAggregate.count", M{})
				Expect(notificationsCount.Error()).Should(BeNil())
				Expect(notificationsCount.Int()).Should(Equal(0))

				//restore snapshot
				<-bkr.Call("notificationsAggregate.restore", M{"snapshotID": snapshotID})

				//aggregate should have 50 records after restore
				waitForRecords(bkr, "notificationsAggregate.count", 50)

				close(done)
			}, 3)
		})

	})

})
