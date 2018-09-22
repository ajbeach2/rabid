package main

import (
	"fmt"
	"github.com/streadway/amqp"
	"log"
	// "os"
	"sync"
)

const exchange = "pubsub"
const cpus = 8

type Session struct {
	*amqp.Connection
	*amqp.Channel
}

type message []byte

type Worker struct {
	Session Session
	In      chan message
	Out     chan message
}

type ConnectionGroup struct {
	Workers    []Worker
	Url        string
	Connection *amqp.Connection
}

func (worker *Worker) NewSession(conn *amqp.Connection) error {
	ch, err := conn.Channel()
	worker.Session = Session{conn, ch}
	if err != nil {
		log.Fatalf("cannot create channel: %v", err)
	}
	if err := worker.Session.Channel.ExchangeDeclare(exchange, "fanout", true, false, false, false, nil); err != nil {
		log.Fatalf("cannot declare fanout exchange: %v", err)
	}
	return err
}

func (grp *ConnectionGroup) Close() error {
	if grp.Connection == nil {
		return nil
	}
	return grp.Connection.Close()
}

func (worker *Worker) Subscribe() {
	queue := "test"

	sub := worker.Session

	if _, err := sub.QueueDeclare(queue, true, false, false, false, nil); err != nil {
		log.Printf("cannot consume from exclusive queue: %q, %v", queue, err)
		return
	}

	routingKey := "application specific routing key for fancy toplogies"
	if err := sub.QueueBind(queue, routingKey, exchange, false, nil); err != nil {
		log.Printf("cannot consume without a binding to exchange: %q, %v", exchange, err)
		return
	}

	deliveries, err := sub.Consume(queue, "", false, false, false, false, nil)
	if err != nil {
		log.Printf("cannot consume from: %q, %v", queue, err)
		return
	}

	log.Printf("subscribed...")
	for msg := range deliveries {
		// fmt.Fprintln(os.Stdout, string(msg.Body))
		sub.Ack(msg.DeliveryTag, false)
	}

}

func (worker *Worker) Publish() {
	var (
		running bool
		reading = worker.In
		pending = make(chan message, 1)
		confirm = make(chan amqp.Confirmation, 1)
	)

	if err := worker.Session.Confirm(false); err != nil {
		log.Printf("publisher confirms not supported")
		close(confirm) // confirms not supported, simulate by always nacking
	} else {
		worker.Session.NotifyPublish(confirm)
	}
Publish:
	for {
		var body message
		select {
		case confirmed, ok := <-confirm:
			if !ok {
				break Publish
			}
			if !confirmed.Ack {
				log.Printf("nack message %d, body: %q", confirmed.DeliveryTag, string(body))
			}
			reading = worker.In

		case body = <-pending:
			routingKey := "ignored for fanout exchanges, application dependent for other exchanges"
			err := worker.Session.Publish(exchange, routingKey, false, false, amqp.Publishing{
				Body: body,
			})
			// Retry failed delivery on the next session
			if err != nil {
				log.Println("Error", err)
				pending <- body
				worker.Session.Channel.Close()
				break Publish
			}

		case body, running = <-reading:
			// all messages consumed
			if !running {
				return
			}
			// work on pending delivery until ack'd
			pending <- body
			reading = nil
		}
	}
}

func (grp *ConnectionGroup) Connect() {
	for {
		err := grp.Close()
		if err != nil {
			log.Fatalf("Error closing connection: %v: %q", err, grp.Url)
		}

		conn, err := amqp.Dial(grp.Url)
		if err != nil {
			log.Fatalf("cannot (re)dial: %v: %q", err, grp.Url)
		} else {
			grp.Connection = conn
		}

		log.Println("Establishing new connection...")

		var wg sync.WaitGroup
		for i, worker := range grp.Workers {
			wg.Add(1)
			log.Println("Creaging publisher", i+1)
			worker.NewSession(grp.Connection)
			go func(x Worker) {
				defer wg.Done()
				x.Publish()
			}(worker)
		}

		for i, worker := range grp.Workers {
			wg.Add(1)
			log.Println("Creaging Subscriber...", i+1)
			worker.NewSession(grp.Connection)
			go func(x Worker) {
				defer wg.Done()
				x.Subscribe()
			}(worker)
		}
		fmt.Println("Waiting for workers to complete...")
		wg.Wait()
	}
}

func NewConnection(url string, in chan message) *ConnectionGroup {
	workers := make([]Worker, cpus, cpus)

	for i := 0; i < cpus; i++ {
		workers[i] = Worker{}
		workers[i].In = in
	}
	connGrp := &ConnectionGroup{}
	connGrp.Url = url
	connGrp.Workers = workers
	return connGrp
}

func main() {

	in := make(chan message)
	connGrp := NewConnection("amqp://localhost:5672", in)

	go func() {
		for i := 0; i < 1000000; i++ {
			in <- []byte(fmt.Sprint(i))
		}
	}()
	connGrp.Connect()
}
