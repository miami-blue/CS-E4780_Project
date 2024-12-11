#### Important Info
This solution does not work in its entirity. The Kafka part works using Docker, but the Flink portion does not function as intended.

#### Dependencies
This repo is missing a few dependencies that the code requires to function:
- flink-connector-kafka-3.4.0-1.20.jar
- kafka-clients-3.9.0.jar
- a Python virtual environment (version 3.11.6 was used for development)
