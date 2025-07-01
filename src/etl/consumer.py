from kafka import KafkaConsumer

consumer = KafkaConsumer("thanhdepzai", group_id = "group", bootstrap_servers = ['localhost:9092'])
running = True
while running:
    msg = consumer.poll(timeout_ms=500)
    for tp, messages in msg.items():
        for message in messages:
            print("%s:%d:%d: key=%s value=%s" % (tp.topic, tp.partition, message.offset, message.key, message.value.decode('utf-8')))