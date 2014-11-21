How to run the streaming API server
===================================

This is how to run the streaming API server
    cd ../src
    python StreamingMarconiServerProtocol.py <host-to-listen-on> <port-to-list-on> <driver-class-name> <driver-args>

Note that <driver-args> is exactly one argument.  Two drivers are currently available:

| **driver class name**  | **description** | **driver-args** | **notes** |
| MockDriver | fake driver -- invents some messages in response to any consume request; any received messages are sent to the bit bucket for recycling; debug messages are printed | none (args are ignored) | some behavior related params are hard-coded in class `__init__`; feel free to update code to make those accessible from driver-args |
| KafkaDriver | implements queue concept using Kafka topics (one queue=one topic) | `<kafka-broker-host>:<kafka-broker-port>` | * topic must already exist
* in consume direction if queue name ends with `/<partition-num>`, then we will only read messages from partition `<partition-num>` ; this is needed currently for cooperative reads from a topic due to kafka-python limitation |

## Examples

Run streaming API server on port 9001 with MockDriver:
    python StreamingMarconiServerProtocol.py localhost 9001 MockDriver “”

Run streaming API server on port 9000 using Kafka running on port 9092 on same machine:
    python StreamingMarconiServerProtocol.py localhost 9000 KafkaDriver localhost:9092
