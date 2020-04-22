package br.com.alura.ecommerce;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import java.io.Closeable;
import java.time.Duration;
import java.util.Collections;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.regex.Pattern;

class KafkaService<T> implements Closeable {
    private final KafkaConsumer<String, Message<T>> consumer;
    private final ConsumerFunction<T> parse;

    public KafkaService(String groupId, String topic, ConsumerFunction<T> parse, Map<String, String> extraProperties) {
        this(parse, groupId, extraProperties);
        consumer.subscribe(Collections.singletonList(topic));
    }

    public KafkaService(String groupId, Pattern topic, ConsumerFunction<T> parse, Map<String, String> extraProperties) {
        this(parse, groupId, extraProperties);
        consumer.subscribe(topic);
    }

    private KafkaService(ConsumerFunction<T> parse, String groupId, Map<String, String> extraProperties) {
        this.parse = parse;
        this.consumer = new KafkaConsumer<>(getProperties(groupId, extraProperties));
    }

    void run() throws ExecutionException, InterruptedException {
        try (var deadLetterDipatcher = new KafkaDispatcher<>()) {
            while (true) {
                var records = consumer.poll(Duration.ofMillis(100));
                if(!records.isEmpty()) {
                    System.out.println("Encontrei " + records.count() + " Registros");

                    for (var record: records) {
                        try {
                            parse.consume(record);
                        } catch (Exception e) {
                            e.printStackTrace();

                            var message = record.value();
                            deadLetterDipatcher.send("ECOMMERCE_DEADLETTER", message.getId().toString(),
                                message.getId().continueWith("DeadLetter"),
                                new GsonSerializer<>().serialize("", message));
                            // If when to send a new message to DEAD LETTER the program has broken, the program must to be interrupted.
                        }
                    }
                }
            }
        }
    }

    private Properties getProperties(String groupId, Map<String, String> overrideProperties) {
        var properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, GsonDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        properties.setProperty(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "1");
        properties.putAll(overrideProperties);
        return properties;
    }

    @Override
    public void close() {
        consumer.close();
    }
}
