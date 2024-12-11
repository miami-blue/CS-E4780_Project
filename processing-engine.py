from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors import FlinkKafkaConsumer, FlinkKafkaProducer
from pyflink.datastream.window import TumblingProcessingTimeWindows
from pyflink.common import Time
from pyflink.common.typeinfo import Types
from pyflink.datastream.functions import ProcessWindowFunction
from datetime import datetime
import json
from pyflink.common.serialization import SimpleStringSchema

class EMAFunction(ProcessWindowFunction):
    def __init__(self):
        self.short_ema_factor = 2 / (1 + 38)
        self.long_ema_factor = 2 / (1 + 100)
        self.ema_short = {}
        self.ema_long = {}

    def process(self, key, context, elements, collector):
        symbol = key
        prices = [float(event["Last"]) for event in elements]
        
        last_price = prices[-1] if prices else 0.0
        self.ema_short[symbol] = last_price * self.short_ema_factor + self.ema_short.get(symbol, 0) * (1 - self.short_ema_factor)
        self.ema_long[symbol] = last_price * self.long_ema_factor + self.ema_long.get(symbol, 0) * (1 - self.long_ema_factor)
        
        short_ema_prev = self.ema_short.get(symbol, 0)
        long_ema_prev = self.ema_long.get(symbol, 0)

        # Detect breakout patterns
        if self.ema_short[symbol] > self.ema_long[symbol] and short_ema_prev <= long_ema_prev:
            collector.collect(f"Buy alert for {symbol} at {datetime.now().isoformat()}")
        elif self.ema_short[symbol] < self.ema_long[symbol] and short_ema_prev >= long_ema_prev:
            collector.collect(f"Sell alert for {symbol} at {datetime.now().isoformat()}")

def main():
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(1)
    env.add_jars("file:///Users/oskari/Code/CS-E4780%20Scalable%20Systems%20and%20Data%20Management/attempt2/flink-connector-kafka-3.4.0-1.20.jar")
    env.add_classpaths("file:///Users/oskari/Code/CS-E4780%20Scalable%20Systems%20and%20Data%20Management/attempt2/flink-connector-kafka-3.4.0-1.20.jar")
    env.add_jars("file:///Users/oskari/Code/CS-E4780%20Scalable%20Systems%20and%20Data%20Management/attempt2/kafka-clients-3.9.0.jar")

    kafka_consumer = FlinkKafkaConsumer(
        topics="financial_data",
        deserialization_schema=SimpleStringSchema(),
        properties={"bootstrap.servers": "localhost:9092", "group.id": "flink_consumer"}
    )

    kafka_producer = FlinkKafkaProducer(
        topic="alerts",
        serialization_schema=SimpleStringSchema(),
        producer_config={"bootstrap.servers": "localhost:9092"}
    )

    stream = env.add_source(kafka_consumer)

    def parse_data(event):
        try:
            return json.loads(event)
        except json.JSONDecodeError:
            return {}

    parsed_stream = stream.map(parse_data, output_type=Types.MAP(Types.STRING(), Types.FLOAT()))
    
    parsed_stream.key_by(lambda x: str(x.get("ID", "RDSA.NL"))) \
        .window(TumblingProcessingTimeWindows.of(Time.milliseconds(30000))) \
        .process(EMAFunction()) \
        .print()  # Print alerts to terminal

    env.execute("EMA Calculation and Breakout Alerts")

if __name__ == "__main__":
    main()
