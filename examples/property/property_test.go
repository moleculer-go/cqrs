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

	It("should transform created events and save as aggregate records", func(done Done) {
		bkr := broker.New(&moleculer.Config{
			LogLevel: logLevel,
		})
		bkr.Publish(Service)
		bkr.Start()

		//create a property
		<-bkr.Call("property.create", M{
			"name":      "Beach Villa",
			"active":    true,
			"bathrooms": 1.5,
			"city":      "Wanaka",
		})

		//check aggregate
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

		bkr.Stop()
		close(done)
	}, 3)

})
