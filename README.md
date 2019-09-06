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
 - a utility to monitor events and/or commands.

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

### Base class `Service`

*Coming soon…*
