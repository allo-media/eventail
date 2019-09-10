# A base class for asynchoronous services within the Allo-Media event-driven architecture and debug utilities.

## Overview

The Allo-Media event-driven service architecture is language and framework independent and is described in the following documents:

* [Conceptual design](https://hackmd.allo-media.net/5v-gV6uGSoOV-C-bogH0NA)
* [Concrete design on RabbitMQ](https://hackmd.allo-media.net/V8sTyOUQRLqaSsMVW7qJ1g)

This base class is a python implementation, fully asynchronous, of a base service conforming to this architecture on RabbitMQ.
To develop a service, just inherit the class and provide a concrete implementation of the abstract methods to code the specific behavior
of your service. All the burden of communication, message safety, recovery, availability, load balancing and queue/exchange setup is taken care for you by this base class and RabbitMQ. It also provide a kind of supervisor to ensure automatic reconnection after a network failure.

You just need to focus on the service logic you develop and then you can deploy as many instances of your service as you see fit, anywhere on the AM network, and the load will be automatically load-balanced between them.

This package also provide some debugging command line tools :

 - a logger that can target specific logs (service & criticity) through topic subscription;
 - a utility to send events on the bus;
 - a utility to send a command on the bus, wait for its result and display the outcome;
 - a utility to monitor events and/or commands;
 - a utility to inspect queues;
 - and a utility to resurrect (replay) dead messages.

### Note about dead letters

The base code  does not create the dead-letters exchange (DLX) for you, nor the queues. It's good practice to do it once and configure the queues with a policy :

```
rabbitmqctl set_policy DLX ".*_events" '{"dead-letter-exchange":"am-dlx"}' --apply-to queues
```

```
rabbitmqctl set_policy DLX ".*_cmds" '{"dead-letter-exchange":"am-dlx"}' --apply-to queues
```

Note that a policy applies to existing **and future** queues as well, so you don't have to reissue those commands each time a new service appears!

## Usage

### Utilities

Once installed (`pip install -e .`), these commands are in you virtualenv path:

```
logger.py --help
usage: logger.py [-h] [--filter [FILTER [FILTER ...]]] amqp_url

Display selected logs in realtime on the given broker

positional arguments:
  amqp_url              URL of the broker, including credentials

optional arguments:
  -h, --help            show this help message and exit
  --filter [FILTER [FILTER ...]]
                        Log patterns to subscribe to (default to all)
```

```
monitor.py --help
usage: monitor.py [-h] [--events [EVENTS [EVENTS ...]]]
                  [--commands [COMMANDS [COMMANDS ...]]]
                  amqp_url

Monitor selected Events and/or Commands on the given broker

positional arguments:
  amqp_url              URL of the broker, including credentials

optional arguments:
  -h, --help            show this help message and exit
  --events [EVENTS [EVENTS ...]]
                        Event patterns to subscribe to (default to all)
  --commands [COMMANDS [COMMANDS ...]]
                        Command patterns to subscribe to (default to all)
```

```
publish_event.py --help
usage: publish_event.py [-h] amqp_url event payload

Publish an Event and its payload on the given broker

positional arguments:
  amqp_url    URL of the broker, including credentials
  event       Event Name
  payload     The path to the file containing the payload, in JSON or
              CBOR format (from file extension).

optional arguments:
  -h, --help  show this help message and exit
```

```
send_command.py --help
usage: send_command.py [-h] amqp_url command payload

Send a service command and its payload on the given broker and waits for its result.

positional arguments:
  amqp_url    URL of the broker, including credentials
  command     Command in the form service.command
  payload     The path to the file containing the payload, in JSON or
              CBOR format (from file extension).

optional arguments:
  -h, --help  show this help message and exit
```

```
inspect_queue.py --help
usage: inspect_queue.py [-h] [--count COUNT] amqp_url queue

Dump the content of a queue without consuming it.

positional arguments:
  amqp_url       URL of the broker, including credentials.
  queue          Name of queue to inspect.

optional arguments:
  -h, --help     show this help message and exit
  --count COUNT  Number of message to dump.
  ```

```
resurrect.py --help
usage: resurrect.py [-h] [--count COUNT] amqp_url queue

Resend dead letters.

positional arguments:
  amqp_url       URL of the broker, including credentials.
  queue          Name of dead-letter queue.

optional arguments:
  -h, --help     show this help message and exit
  --count COUNT  Number of message to resurrect (default is 0 = all).
```


### Base class `Service`

*Documentation coming soon, meanwhile you can read the code.*
