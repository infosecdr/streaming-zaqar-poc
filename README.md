streaming-zaqar-poc
===================

A proof of concept of a Websocket-based queuing streaming API based on OpenStack Zaqar, with a working Kafka driver.

## Background

OpenStack Zaqar (https://wiki.openstack.org/wiki/Zaqar; until recently called Marconi) is a multi-tenant cloud messaging service for web developers.  Symantec's Cloud Platform Engineering group (http://cpe.symantec.com), which has built a cloud platform on OpenStack, had the need for a multi-tenant interface for message queues in conjunction with a stream processing service.  Thus, we started looking at Zaqar's interface.

One issue we saw with using Zaqar was that it only has a REST API and we knew that would not be performant enough for storing and retrieving message from the service at the rate of tens of thousands of messages per second.  To be performant enough, it would have to a streaming interface (long lived connections with minimal overhead and in which messages can be sent over time).

As a proof of concept (PoC), we did some research and developed a Websocket-based streaming API that is intended to complement the existing Zaqar REST API.  This was our own take on what a streaming interface for Zaqar would look like.  This protocol keeps the same elements and semantics as the Zaqar REST API.

As a further PoC, we wrote some server-side and client-side producer and consumer code to fill the different ends of the protocol the API.  We also wrote a driver for using Kafka for message storage.

We provided the Zaqar team with our streaming API and they expressed an interest in adding a streaming API based on Websocket.  Thus we 

Since this streaming API will soon be superceded by an official Zaqar streaming API, we are not doing further development of our own streaming API and instead will be supporting the official one.  Symantec plans to developing an open source Kafka driver for Zaqar.

## Some notes

*  The API/protocol we developed doesn't really have a distinct name; we just refer to it as "Queuing Streaming Interface" or "High Performance Queuing Streaming API" or simply "streaming API".
* Kafka cannot support everything Zaqar and we have requirements for things not present in Zaqar, so our Kafka driver doesn't strictly conform to the defined semantics of Zaqar.
* This was originally written as internal prototype, so this is not as refined as it would be if it was expected to be public or if it was production code.
* We have not done a formal performance evaluation of our PoC so we don't have a objective measure of how fast it is.
* We have not used the PoC code extensively but we were able to demo it with two producers, two consumers, and two servers running at the same time and on the same host and with messages stored in Kafka.
* Sorry that some of the docs here are currently in MS Word.  Google Docs offers limited export options.
* Contributions are quite welcome

## Organization of this repo

The subdirectories of this repro are:
* protocol: contains files related to the Queuing Streaming Interface (start here)
* server: contains files related to the server-side PoC implementation of the Queuing Streaming Interface (both server interface and drivers)
* client: contains files related to PoC producer and consumer clients that can work with a Queuing Streaming Interface server

## Building the code

Do these in order:
    protocol/src/recompile-protobuf.sh
    (cd clients; mvn clean package)

## Running the PoC

In separate shells:
* cd to server/docs and follow the directions in running-the-service.md for how to start the server
* cd to "client" and see the README.md for how to run the producing and consuming clients
