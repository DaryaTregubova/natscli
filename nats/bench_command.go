// Copyright 2020 The NATS Authors
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package main

import (
	"bytes"
	"fmt"
	"io/ioutil"
	"log"
	"sync"
	"time"

	"github.com/dustin/go-humanize"
	"github.com/gosuri/uiprogress"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/bench"
	"gopkg.in/alecthomas/kingpin.v2"
)

type benchCmd struct {
	subject        string
	numPubs        int
	numSubs        int
	numMsg         int
	msgSize        int
	csvFile        string
	progress       bool
	request        bool
	js             bool
	jsFile         bool
	pull           bool
	ack            bool
	replicas       int
	noPurge        bool
	noDeleteStream bool
}

const (
	JS_STREAM_NAME       string = "benchstream"
	JS_PUSHCONSUMER_NAME string = "pushconsumer"
	JS_PULLCONSUMER_NAME string = "pullconsumer"
)

func configureBenchCommand(app *kingpin.Application) {
	c := &benchCmd{}
	bench := app.Command("bench", "Benchmark utility").Action(c.bench)
	bench.Arg("subject", "Subject to use for testing").Required().StringVar(&c.subject)
	bench.Flag("pub", "Number of concurrent publishers").Default("1").IntVar(&c.numPubs)
	bench.Flag("sub", "Number of concurrent subscribers").Default("0").IntVar(&c.numSubs)
	bench.Flag("msgs", "Number of messages to publish").Default("100000").IntVar(&c.numMsg)
	bench.Flag("size", "Size of the test messages").Default("128").IntVar(&c.msgSize)
	bench.Flag("csv", "Save benchmark data to CSV file").StringVar(&c.csvFile)
	bench.Flag("progress", "Enable progress bar while publishing").Default("true").BoolVar(&c.progress)
	bench.Flag("request", "Waits for acknowledgement on messages using Requests rather than Publish").Default("false").BoolVar(&c.request)
	bench.Flag("streaming", "Use JetStream streaming").Default("false").BoolVar(&c.js)
	bench.Flag("jsfile", "Persist the stream to file").Default("false").BoolVar(&c.jsFile)
	bench.Flag("pull", "Uses JS pull consumers").Default("false").BoolVar(&c.pull)
	bench.Flag("ack", "Acks consumption of messages").Default("false").BoolVar(&c.ack)
	bench.Flag("replicas", "Number of stream replicas").Default("1").IntVar(&c.replicas)
	bench.Flag("nopurge", "Do not purge the stream before running").Default("false").BoolVar(&c.noPurge)
	bench.Flag("nodelete", "Do not delete the stream at the end of the run").Default("false").BoolVar(&c.noDeleteStream)

	cheats["bench"] = `# benchmark JetStream acknowledged publishes
nats bench --request --msgs 10000 ORDERS.bench

# benchmark core nats publish and subscribe with 10 publishers and subscribers
nats bench --pub 10 --sub 10 --msgs 10000 --size 512
`
}

func (c *benchCmd) bench(_ *kingpin.ParseContext) error {
	if c.numMsg <= 0 {
		return fmt.Errorf("number of messages should be greater than 0")
	}

	log.Printf("Starting benchmark [msgs=%s, msgsize=%s, pubs=%d, subs=%d, js=%v, jsfile=%v, request=%v, pull=%v, ack=%v, replicas=%d, nopurge=%v, nodelete=%v]", humanize.Comma(int64(c.numMsg)), humanize.IBytes(uint64(c.msgSize)), c.numPubs, c.numSubs, c.js, c.jsFile, c.request, c.pull, c.ack, c.replicas, c.noPurge, c.noDeleteStream)

	if c.request && c.progress {
		log.Printf("Disabling progress bars in request mode")
		c.progress = false
	}

	bm := bench.NewBenchmark("NATS", c.numSubs, c.numPubs)

	startwg := &sync.WaitGroup{}
	donewg := &sync.WaitGroup{}

	var js nats.JetStreamContext

	if c.js {
		// create the stream for the benchmark (and purge it)
		nc, err := nats.Connect(config.ServerURL(), natsOpts()...)
		if err != nil {
			log.Fatalf("nats connection failed: %s", err)
			return err
		}

		js, err = nc.JetStream()
		if err != nil {
			log.Fatalf("couldn't create jetstream context: %v", err)
		}

		storageType := func() nats.StorageType {
			if c.jsFile {
				return nats.FileStorage
			} else {
				return nats.MemoryStorage
			}
		}()

		js.AddStream(&nats.StreamConfig{Name: JS_STREAM_NAME, Subjects: []string{c.subject}, Retention: nats.LimitsPolicy, MaxConsumers: -1, MaxMsgs: -1, MaxBytes: -1, Discard: nats.DiscardOld, MaxAge: 9223372036854775807, MaxMsgsPerSubject: -1, MaxMsgSize: -1, Storage: storageType, Replicas: c.replicas, Duplicates: time.Second * 2})
		if !c.noPurge {
			log.Printf("Purging the stream")
			js.PurgeStream(JS_STREAM_NAME)
		}

		if !c.noDeleteStream {
			log.Printf("Will delete the stream at the end of the run")
			defer js.DeleteStream(JS_STREAM_NAME)
		}

		// create the pull consumer
		if c.pull && c.numSubs > 0 {
			js.AddConsumer(JS_STREAM_NAME, &nats.ConsumerConfig{
				Durable:       JS_PULLCONSUMER_NAME,
				DeliverPolicy: nats.DeliverAllPolicy,
				AckPolicy:     nats.AckExplicitPolicy,
				ReplayPolicy:  nats.ReplayInstantPolicy,
			})
			defer js.DeleteConsumer(JS_STREAM_NAME, JS_PULLCONSUMER_NAME)
		}
	}

	subCounts := bench.MsgsPerClient(c.numMsg, c.numSubs)

	for i := 0; i < c.numSubs; i++ {
		nc, err := nats.Connect(config.ServerURL(), natsOpts()...)
		if err != nil {
			return fmt.Errorf("nats connection %d failed: %s", i, err)
		}
		defer nc.Close()

		startwg.Add(1)
		donewg.Add(1)

		numMsg := func() int {
			if c.pull {
				return subCounts[i]
			} else {
				return c.numMsg
			}
		}()

		go c.runSubscriber(bm, nc, startwg, donewg, numMsg)
	}
	startwg.Wait()

	// create push consumer if needed
	if js != nil && c.numSubs > 0 && !c.pull {
		js.AddConsumer(JS_STREAM_NAME, &nats.ConsumerConfig{
			Durable:        JS_PUSHCONSUMER_NAME,
			DeliverSubject: c.subject + ".pushconsumer",
			DeliverPolicy:  nats.DeliverAllPolicy,
			AckPolicy:      nats.AckAllPolicy,
			ReplayPolicy:   nats.ReplayInstantPolicy,
		})
		defer js.DeleteConsumer(JS_STREAM_NAME, JS_PUSHCONSUMER_NAME)
	}

	pubCounts := bench.MsgsPerClient(c.numMsg, c.numPubs)

	for i := 0; i < c.numPubs; i++ {
		nc, err := nats.Connect(config.ServerURL(), natsOpts()...)
		if err != nil {
			return fmt.Errorf("nats connection %d failed: %s", i, err)
		}
		defer nc.Close()

		startwg.Add(1)
		donewg.Add(1)

		go c.runPublisher(bm, nc, startwg, donewg, pubCounts[i])
	}

	if c.progress {
		uiprogress.Start()
	}

	startwg.Wait()
	donewg.Wait()

	bm.Close()

	if c.progress {
		uiprogress.Stop()
	}

	fmt.Println()
	fmt.Println(bm.Report())

	if c.csvFile != "" {
		csv := bm.CSV()
		ioutil.WriteFile(c.csvFile, []byte(csv), 0644)
		fmt.Printf("Saved metric data in csv file %s\n", c.csvFile)
	}

	return nil
}

func (c *benchCmd) runPublisher(bm *bench.Benchmark, nc *nats.Conn, startwg *sync.WaitGroup, donewg *sync.WaitGroup, numMsg int) {
	startwg.Done()

	var progress *uiprogress.Bar
	if c.progress {
		progress = uiprogress.AddBar(numMsg).AppendCompleted().PrependElapsed()
		progress.Width = progressWidth()
	} else {
		log.Printf("Starting publisher, publishing %s messages", humanize.Comma(int64(numMsg)))
	}

	var msg []byte
	if c.msgSize > 0 {
		msg = make([]byte, c.msgSize)
	}

	start := time.Now()

	var m *nats.Msg
	var err error

	errBytes := []byte("error")
	minusByte := byte('-')

	for i := 0; i < numMsg; i++ {
		if progress != nil {
			progress.Incr()
		}

		if !c.request {
			nc.Publish(c.subject, msg)
			continue
		}

		m, err = nc.Request(c.subject, msg, time.Second)
		if err != nil {
			log.Println(err)
			continue
		}

		if len(m.Data) == 0 || m.Data[0] == minusByte || bytes.Contains(m.Data, errBytes) {
			log.Printf("Did not receive a positive ACK: %q", m.Data)
		}
	}

	nc.Flush()

	bm.AddPubSample(bench.NewSample(numMsg, c.msgSize, start, time.Now(), nc))

	donewg.Done()
}

func (c *benchCmd) runSubscriber(bm *bench.Benchmark, nc *nats.Conn, startwg *sync.WaitGroup, donewg *sync.WaitGroup, numMsg int) {
	received := 0
	ch := make(chan time.Time, 2)

	mh := func(msg *nats.Msg) {
		received++
		if c.js && c.ack {
			msg.Ack()
		}
		if received == 1 {
			ch <- time.Now()
		}
		if received >= numMsg {
			ch <- time.Now()
		}
	}
	var sub *nats.Subscription

	log.Printf("Starting subscriber, expecting %s messages", humanize.Comma(int64(numMsg)))

	if c.js {
		js, err := nc.JetStream()
		if err != nil {
			log.Fatalf("couldn't create jetstream context: %v", err)
		}

		if c.pull {
			//sub, _ = js.SubscribeSync(c.subject)
			sub, err = js.PullSubscribe(c.subject, JS_PULLCONSUMER_NAME)
			if err != nil {
				println("error pullsubscribe=" + err.Error())
			}
		} else {
			sub, _ = nc.Subscribe(c.subject+".pushconsumer", mh)
		}
	} else {
		sub, _ = nc.Subscribe(c.subject, mh)
	}

	sub.SetPendingLimits(-1, -1)
	nc.Flush()
	startwg.Done()

	if c.js && c.pull {
		for i := 0; i < numMsg; i++ {
			msg, err := sub.NextMsg(time.Second * 3)
			if err == nil {
				mh(msg)
			} else {
				log.Fatalf("pull consumer timed out")
			}
		}
	}

	start := <-ch
	end := <-ch

	bm.AddSubSample(bench.NewSample(c.numMsg, c.msgSize, start, end, nc))

	if sub != nil {
		sub.Unsubscribe()
	}

	nc.Close()
	donewg.Done()
}
