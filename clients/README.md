Java clients for Queuing Streaming Interface
============================================

This directory contains Java-based CLI clients for the Queuing Streaming Interface.

(Sorry for the scarce documentation; feel free to ask questions.)

## Usage

First do a "mvn clean package"

### Produce-direction

Run it like:

    java -cp target/streaming_api_client-1.0-SNAPSHOT-jar-with-dependencies.jar com.symantec.cpe.spaas.streaming.api.client.WebsocketStreamingProducerClient <streamingServerHost> <streamingServerPort> <queueName>

For example:

    java -cp target/streaming_api_client-1.0-SNAPSHOT-jar-with-dependencies.jar com.symantec.cpe.spaas.streaming.api.client.WebsocketStreamingProducerClient localhost 9001 topic1

This will generate a stream of messages ("message #NNN") and send them to the server.

### Consume-direction

Run it like:

    java -cp target/streaming_api_client-1.0-SNAPSHOT-jar-with-dependencies.jar com.symantec.cpe.spaas.streaming.api.client.WebsocketStreamingConsumerClient <streamingServerHost> <streamingServerPort> <queueName> <starting-marker>

For example:

    java -cp target/streaming_api_client-1.0-SNAPSHOT-jar-with-dependencies.jar com.symantec.cpe.spaas.streaming.api.client.WebsocketStreamingConsumerClient localhost 9001 topic1 0

Received messages will be sent to stdout.
