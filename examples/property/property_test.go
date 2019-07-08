package property

import (
	"fmt"

	"github.com/moleculer-go/moleculer"
	"github.com/moleculer-go/moleculer/broker"
	"github.com/moleculer-go/moleculer/payload"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

type M map[string]interface{}

var _ = Describe("Property service", func() {
	logLevel := "fatal"

	createMultipleProperties := func(bkr *broker.ServiceBroker, size int) {
		for index := 1; index <= size; index++ {
			city := "Wanaka"
			if index%2 == 0 {
				city := "Queenstown"
			}
			<-bkr.Call("property.create", M{
				"name":        "Beach Villa Number " + index,
				"active":      true,
				"bathrooms":   1.5,
				"city":        city,
				"countryCode": "NZ",
			})
		}
	}

	It("property.created event be transformed into property and property summary aggregate", func(done Done) {
		bkr := broker.New(&moleculer.Config{
			LogLevel: logLevel,
		})
		bkr.Publish(Service)
		bkr.Start()

		//create a property
		<-bkr.Call("property.create", M{
			"name":        "Beach Villa",
			"active":      true,
			"bathrooms":   1.5,
			"city":        "Wanaka",
			"countryCode": "NZ",
		})

		//check property aggregate
		r := payload.Empty()
		//wait for one record to be created in the aggregate :)
		for {
			r = <-bkr.Call("propertyAggregate.find", M{"query": M{"name": "Beach Villa"}})
			Expect(r.Error()).Should(Succeed())
			if r.Len() == 1 {
				break
			}
		}
		Expect(r.First().Get("name").String()).Should(Equal("Beach Villa"))
		Expect(r.First().Get("bathrooms").Float()).Should(Equal(1.5))
		Expect(r.First().Get("city").String()).Should(Equal("Wanaka"))
		fmt.Print("raw active ", r.First().Get("active").Value())
		Expect(r.First().Get("active").Bool()).Should(Equal(true))

		//check country summary aggregate
		r = payload.Empty()
		//eventualy consistent :) wait for one record to be created in the aggregate :)
		for {
			r = <-bkr.Call("propertySummaryAggregate.find", M{})
			Expect(r.Error()).Should(Succeed())
			if r.Len() == 1 {
				break
			}
		}
		fmt.Println("result: --> ", r)
		Expect(r.First().Get("beachCity").Int()).Should(Equal(0))
		Expect(r.First().Get("mountain").Int()).Should(Equal(1))
		Expect(r.First().Get("total").Int()).Should(Equal(1))
		Expect(r.First().Get("countryCode").String()).Should(Equal("NZ"))

		bkr.Stop()
		close(done)
	}, 1)

	It("propertyAggregate.snapshot should create an snapshot :)", func(done Done) {
		bkr := broker.New(&moleculer.Config{
			LogLevel: logLevel,
		})
		bkr.Publish(Service)
		bkr.Start()

		createMultipleProperties(bkr, 10)

		snapshotName := <-bkr.Call("propertyAggregate.snapshot", M{})
		Expect(snapshotName.Error()).Should(Succeed())
		Expect(snapshotName.String()).ShouldNot(Equal(""))

		//check snapshot event

		//check bkp file created

		//check event pump is back

		bkr.Stop()
		close(done)
	}, 3)

	It("propertyAggregate.restore should restore from a snapshot :)", func(done Done) {
		bkr := broker.New(&moleculer.Config{
			LogLevel: logLevel,
		})
		bkr.Publish(Service)
		bkr.Start()

		create10Properties(bkr)

		snapshotName := <-bkr.Call("propertyAggregate.snapshot", M{})

		<-bkr.Call("propertyAggregate.restore", M{"snapshot": snapshotName})

		//check state of aggregates is correct

		//check event pump is back

		bkr.Stop()
		close(done)
	}, 3)

})
