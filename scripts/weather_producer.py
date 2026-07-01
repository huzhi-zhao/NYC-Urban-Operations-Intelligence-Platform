import json
import random
import signal
import sys
import time

from kafka import KafkaProducer

KAFKA_BOOTSTRAP_SERVERS = ["kafka:9092"]  # container name on bigdata-net
TOPIC = "weather_data"

_running = True


def _handle_sigterm(signum, frame):
    global _running
    _running = False


def main() -> None:
    signal.signal(signal.SIGTERM, _handle_sigterm)
    signal.signal(signal.SIGINT, _handle_sigterm)

    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        value_serializer=lambda m: json.dumps(m).encode("ascii"),
    )

    while _running:
        temperature = round(random.uniform(-20, 40), 1)
        humidity = random.randint(0, 100)
        wind_speed = round(random.uniform(0, 20), 1)
        weather_data = {
            "city": "New York",
            "temperature": temperature,
            "humidity": humidity,
            "wind_speed": wind_speed,
        }
        producer.send(TOPIC, value=weather_data)
        print(f"Sending weather data for New York: {weather_data}", flush=True)

        temperature = round(random.uniform(0, 30), 1)
        humidity = random.randint(0, 100)
        wind_speed = round(random.uniform(0, 20), 1)
        weather_data = {
            "city": "San Francisco",
            "temperature": temperature,
            "humidity": humidity,
            "wind_speed": wind_speed,
        }
        producer.send(TOPIC, value=weather_data)
        print(f"Sending weather data for San Francisco: {weather_data}", flush=True)

        time.sleep(5)

    producer.flush()
    producer.close()


if __name__ == "__main__":
    main()
    sys.exit(0)
