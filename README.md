## Data-Processing - Server program ##
### Notes ###
This is an Eclipse project with Maven dependencies. Should be able to be directly imported into a Eclipse workspace.

### Usage ###
- Follow this link to download and configure Apache Kafka server [http://kafka.apache.org/documentation.html#quickstart]. When setting up a new topic, name it `temperature`. Set the hostname and port according to this quickstart guide.
- Import the Eclipse project into your workspace. Then you would be able to test Kafka producer and consumer by calling `startProducer()` and `startConsumer()` functions.
- `DataProcessing.processKafkaData()` function uses Apache Spark Steaming with Kafka Integration. To feed it data, follow the above quickstart guide to start a Kafka producer in Terminal, with topic `temperature`.
  	