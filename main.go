package main

import (
	"context"
	"encoding/binary"
	"flag"
	"fmt"
	"math/rand"
	"os"
	"strings"
	"sync"
	"time"

	amqp "github.com/Azure/go-amqp"
)

var serverAddr = flag.String("server", "amqp://localhost:5672", "AMQP 1.0 server endpoint")
var messageCount = flag.Int("count", 10, "Number of messages to send/receive")
var pubisherCount = flag.Int("publisherCount", 1, "Number of publishers")
var consumerCount = flag.Int("consumerCount", 1, "Number of consumers")
var interval = flag.Int("interval", 0, "Interval between messages (ms)")
var queueName = flag.String("queue", "amqp10_test", "Destination queue")
var publishOnly = flag.Bool("publishOnly", false, "If true, only publish messages, don't subscribe")
var separateQueues = flag.Bool("separateQueues", false, "If true, each publisher uses a separate queue")
var consumeOnly = flag.Bool("consumeOnly", false, "If true, only consume messages, don't publish")
var timestampBody = flag.Bool("timestampBody", false, "If true, message body is perf-test compatible")
var size = flag.Int("size", 20, "Size of message body (bytes)")
var helpFlag = flag.Bool("help", false, "Print help text")

func main() {
	flag.Parse()
	if *helpFlag {
		fmt.Fprintf(os.Stderr, "Usage of %s\n", os.Args[0])
		flag.PrintDefaults()
		os.Exit(1)
	}

	var wg sync.WaitGroup

	if !*publishOnly {
		for i := 1; i <= *consumerCount; i++ {
			subscribed := make(chan bool)
			n := i
			wg.Add(1)
			go func() {
				defer wg.Done()
				recvMessages(subscribed, n)
			}()

			// wait until we know the receiver has subscribed
			<-subscribed
		}
	}

	if !*consumeOnly {
		for i := 1; i <= *pubisherCount; i++ {
			n := i
			wg.Add(1)
			go func() {
				defer wg.Done()
				sendMessages(n)
			}()
		}
	}

	wg.Wait()
}

func sendMessages(n int) {
	// sleep random interval to avoid all senders connecting at the same time
	s := rand.Intn(n)
	time.Sleep(time.Duration(s) * time.Millisecond)

	conn, err := amqp.Dial(context.TODO(), *serverAddr, nil)
	if err != nil {
		println("cannot connect to server", err.Error())
		return
	}

	session, err := conn.NewSession(context.TODO(), nil)
	if err != nil {
		println("failed to create a session", err)
		return
	}
	var queue string
	if *separateQueues {
		queue = fmt.Sprintf("/queue/%s-%d", *queueName, n)
	} else {
		queue = "/queue/" + *queueName
	}
	sender, err := session.NewSender(context.TODO(), queue, &amqp.SenderOptions{
		Durability: amqp.DurabilityUnsettledState})
	if err != nil {
		println("failed to create a sender", err)
		return
	}
	for i := 1; i <= *messageCount; i++ {
		b := make([]byte, *size)
		var text string
		if *timestampBody {
			binary.BigEndian.PutUint32(b[0:], uint32(1234))
			binary.BigEndian.PutUint64(b[4:], uint64(time.Now().UnixMilli()))
		} else {
			text = fmt.Sprintf("Message #%d:", i)
			copy(b[0:len(text)], []byte(text))
		}
		err = sender.Send(context.TODO(), amqp.NewMessage(b), nil)
		if err != nil {
			println("failed to send to server", err)
			return
		}
		time.Sleep(time.Duration(*interval) * time.Millisecond)
	}
	time.Sleep(1 * time.Second)
	println("sender finished")
}

func recvMessages(subscribed chan bool, n int) {
	conn, err := amqp.Dial(context.TODO(), *serverAddr, nil)

	if err != nil {
		println("cannot connect to server", err.Error())
		return
	}
	println("Connected...")

	session, err := conn.NewSession(context.TODO(), nil)
	if err != nil {
		println("failed to create a session", err)
		return
	}
	var queue string
	if *separateQueues {
		queue = fmt.Sprintf("/queue/%s-%d", *queueName, n)
	} else {
		queue = "/queue/" + *queueName
	}
	receiver, err := session.NewReceiver(context.TODO(), queue, &amqp.ReceiverOptions{Credit: 5})
	if err != nil {
		println("failed to create a receiver", err)
		return
	}
	println("Subscribed...")
	close(subscribed)

	for i := 1; i <= *messageCount; i++ {
		msg, err := receiver.Receive(context.TODO(), nil)
		println("received message", string(msg.GetData()))
		if err != nil {
			println("failed to receive message", err)
			return
		}
		if !*timestampBody {
			expectedText := fmt.Sprintf("Message #%d", i)
			actualText, _, _ := strings.Cut(string(msg.GetData()), ":")
			if expectedText != actualText {
				println("Expected:", expectedText)
				println("Actual:", actualText)
			}
		}
		err = receiver.AcceptMessage(context.TODO(), msg)
		if err != nil {
			return
		}
	}

	println("receiver finished")

}
