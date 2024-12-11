from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors import FlinkKafkaConsumer
from pyflink.datastream.window import TumblingProcessingTimeWindows
from pyflink.common.serialization import SimpleStringSchema
from pyflink.common.typeinfo import Types
from pyflink.datastream.functions import ProcessWindowFunction
from pyflink.common.watermark_strategy import WatermarkStrategy
import json
from pyflink.common import Time

class ProcessValidRows(ProcessWindowFunction):
    def process(self, key, context, elements, collector):
        symbol = key
        last_values = [float(row['Last']) for row in elements if 'Last' in row]
        if last_values:
            collector.collect(f"Symbol: {symbol}, Last Value: {last_values[-1]}")

def main():
    # Set up the execution environment
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(1)
    env.add_jars("file:///flink-connector-kafka-3.4.0-1.20.jar")
    env.add_classpaths("file:///flink-connector-kafka-3.4.0-1.20.jar")
    env.add_jars("file:///kafka-clients-3.9.0.jar")

    # Define Kafka consumer properties
    kafka_consumer_props = {
        'bootstrap.servers': 'localhost:9092',
        'group.id': 'flink_consumer_group'
    }

    # Create a Kafka consumer
    kafka_consumer = FlinkKafkaConsumer(
        topics='trading-data',
        deserialization_schema=SimpleStringSchema(),
        properties=kafka_consumer_props
    )

    # Add the Kafka consumer as a source to the environment
    kafka_stream = env.add_source(kafka_consumer, type_info=Types.STRING())

    # Parse the JSON messages, filter invalid rows, and process valid data
    def parse_and_filter(data):
        try:
            record = json.loads(data)
            # Check for valid fields
            if all(field in record for field in ['ID', 'SecType', 'Last', 'Trading time', 'Trading date']):
                if record['ID'] == 'RDSA.NL':  # Filter for the specific symbol
                    return record
        except json.JSONDecodeError:
            pass
        return None

    valid_stream = kafka_stream \
        .map(parse_and_filter, output_type=Types.MAP(Types.STRING(), Types.STRING())) \
        .filter(lambda x: x is not None)

    # Group by symbol (ID) and process in 5-minute tumbling windows
    valid_stream.key_by(lambda x: x['ID']) \
        .window(TumblingProcessingTimeWindows.of(Time.minutes(0.1))) \
        .process(ProcessValidRows()) \
        .print()

    # Execute the Flink job
    env.execute('Enhanced Kafka Flink Consumer')

if __name__ == '__main__':
    main()
