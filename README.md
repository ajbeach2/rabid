# Rabid

## Requirements
	* Docker-compose
	* github.com/streadway/amqp
## To Test
1. Start rabbit
```make rabbit```

2. Run the go worker
```make run```

Open http://localhost:15672/#/queues
