from kafka import KafkaConsumer
import json
from datetime import datetime

# Initialize dictionaries to store EMA values for each symbol
ema_short = {}
ema_long = {}

# Parameters for EMA calculation
SHORT_EMA_FACTOR = 2 / (1 + 38)
LONG_EMA_FACTOR = 2 / (1 + 100)

# Kafka topic and server configuration
TOPIC = "financial_data"
BOOTSTRAP_SERVERS = "localhost:9092"

def calculate_ema(last_price, prev_ema, smoothing_factor):
    return last_price * smoothing_factor + prev_ema * (1 - smoothing_factor)

def process_message(message):
    global ema_short, ema_long

    # Parse message
    try:
        event = json.loads(message.value)
    except json.JSONDecodeError:
        print("Invalid message format")
        return

    # Extract relevant fields
    symbol = event.get("ID")
    last_price = event.get("Last")
    if not (symbol and last_price):
        return

    try:
        last_price = float(last_price)
    except ValueError:
        print(f"Invalid price for symbol {symbol}: {last_price}")
        return

    # Calculate EMAs
    prev_ema_short = ema_short.get(symbol, 0)
    prev_ema_long = ema_long.get(symbol, 0)

    ema_short[symbol] = calculate_ema(last_price, prev_ema_short, SHORT_EMA_FACTOR)
    ema_long[symbol] = calculate_ema(last_price, prev_ema_long, LONG_EMA_FACTOR)

    # Detect breakout patterns
    if ema_short[symbol] > ema_long[symbol] and prev_ema_short <= prev_ema_long:
        print(f"Buy alert for {symbol} at {datetime.now().isoformat()}")
    elif ema_short[symbol] < ema_long[symbol] and prev_ema_short >= prev_ema_long:
        print(f"Sell alert for {symbol} at {datetime.now().isoformat()}")

def main():
    # Initialize Kafka consumer
    consumer = KafkaConsumer(
        TOPIC,
        bootstrap_servers=BOOTSTRAP_SERVERS,
        auto_offset_reset="earliest",
        value_deserializer=lambda m: m.decode("utf-8")
    )

    print(f"Listening to topic: {TOPIC}")
    for message in consumer:
        process_message(message)

if __name__ == "__main__":
    main()
