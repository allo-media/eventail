# A base class for asynchoronous services within the Allo-Media event-driven service architecture.

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
 - a utility to monitor events;
 - a utility to monitor commands.

## Usage


