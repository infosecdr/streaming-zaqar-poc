server source directory
=======================

This directory contains the following:
* StreamingMarconiServerProtocol.py: run this to start the server; contains bulk of logic for handling requests (the server interface)
* ProtoBufStream.py: used internally in StreamingMarconiServerProtocol
* MessageStream.py: used in the interface between the server interface and drivers
* KafkaDriver.py: if configured, called by the server interface to store or retrieve messages from a Kafka topic
* MockDriver.py: if configured, called by the server interface to store or retrieve messages, but its really just a stub; messages aren't really saves and requested messages are pulled from thin air