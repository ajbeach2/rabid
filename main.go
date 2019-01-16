package main

import (
	"fmt"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
	"github.com/streadway/amqp"
	"log"
	"os"
	"strconv"
	"sync"
)

var exchange string
var workers int
var routingKey string
var queue string

func init() {
	zerolog.TimeFieldFormat = ""
	exchange = os.Getenv("OUTBOUND_EXCHANGE")
	queue = os.Getenv("INBOUD_QUEUE")
	workers = os.Getenv("RABID_WORKERS")
	routingKey = os.Getenv("ROUTING_KEY")

	if exchange == "" {
		log.Fatal("Missing Environment Variable OUTBOUND_EXCHANGE")
	}

	if queue == "" {
		log.Fatal("Missing Environment Variable INBOUD_QUEUE")
	}

	if workers == "" {
		workers = 1
	} else {
		i, err := strconv.Atoi(workers)
		if err != nil {
			log.Fatal(err)
		}
	}
}

type Session struct {
	*amqp.Connection
	*amqp.Channel
}

type message []byte

type Worker struct {
	Session Session
	Out     chan message
}

type App struct {
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
	if err := worker.Session.Channel.ExchangeDeclare(exchange, "fanout", false, false, false, false, nil); err != nil {
		log.Fatalf("cannot declare fanout exchange: %v", err)
	}
	return err
}

func (grp *App) Close() error {
	if grp.Connection == nil {
		return nil
	}
	return grp.Connection.Close()
}

func (worker *Worker) Subscribe() {
	sub := worker.Session

	if _, err := sub.QueueDeclare(queue, false, false, false, false, nil); err != nil {
		log.Printf("cannot consume from exclusive queue: %q, %v", queue, err)
		return
	}

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
		if err := worker.Process(msg); err != nil {
			log.Println(err)
		} else {
			worker.out <- msg
			sub.Ack(msg.DeliveryTag, false)
		}
	}

}

func (worker *Worker) Publish() {
	var (
		running bool
		reading = worker.In
	)
	for {
		var body message
		select {
		case body, running = <-reading:
			if !running {
				return
			}
			err := worker.Session.Publish(exchange, routingKey, false, false, amqp.Publishing{
				Body: body,
			})
			if err != nil {
				log.Println(err)
			}
		}
	}
}

func (grp *App) Run() {
	for {
		err := grp.Close()
		if err != nil {
			log.Fatalf("Error closing connection: %v: %q", err, grp.Url)
		}

		log.Println("Establishing new connection...")
		conn, err := amqp.Dial(grp.Url)
		if err != nil {
			log.Fatalf("cannot (re)dial: %v: %q", err, grp.Url)
		} else {
			log.Println("New connection established")
			grp.Connection = conn
		}

		var wg sync.WaitGroup
		for i, worker := range grp.Workers {
			wg.Add(1)
			log.Println("Creating publisher...", i+1)
			worker.NewSession(grp.Connection)
			go func(x Worker) {
				defer wg.Done()
				x.Publish()
			}(worker)
		}

		for i, worker := range grp.Workers {
			wg.Add(1)
			log.Println("Creating Subscriber...", i+1)
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

type AppConfig struct {
	URL              string
	Process          func([]byte) ([]byte, error)
	Workers          int
	OutBoundExchange string
	InBoundQueue     string
	RoutingKey       string
}

func NewApp(config *AppConfig) *App {
	workers := make([]Worker, workers, config.Workers)
	out := make(chan message)

	for i := 0; i < config.Workers; i++ {
		workers[i] = Worker{}
		workers[i].out = Out
		workers[i].Process = config.Process
	}
	connGrp := &App{}
	connGrp.Url = url
	connGrp.Workers = workers
	return connGrp
}

func main() {
	connGrp := NewApp("amqp://localhost:5672", in)
	connGrp.Run()
}
