from kafka import KafkaProducer
import time

producer = KafkaProducer(bootstrap_servers='localhost:9092')
words = ['hello', 'world', 'foo', 'bar', 'baz']
i = 0

try:
    while True:
        word = words[i % len(words)]
        producer.send('words', word.encode('utf-8'))
        print(f"Sent → {word}")
        time.sleep(1)
        i += 1
except KeyboardInterrupt:
    producer.close()
