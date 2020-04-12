package br.com.alura.ecommerce;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.io.Closeable;
import java.time.Duration;
import java.util.Collections;
import java.util.Map;
import java.util.Properties;
import java.util.regex.Pattern;

class KafkaService<T> implements Closeable {
    private final KafkaConsumer<String, Message<T>> consumer;
    private final ConsumerFunction<T> parse;

    public KafkaService(String groupId, String topic, ConsumerFunction<T> parse, Class<T> type, Map<String, String> extraProperties) {
        this(parse, groupId, type, extraProperties);
        consumer.subscribe(Collections.singletonList(topic));
    }

    public KafkaService(String groupId, Pattern topic, ConsumerFunction<T> parse, Class<T> type, Map<String, String> extraProperties) {
        this(parse, groupId, type, extraProperties);
        consumer.subscribe(topic);
    }

    private KafkaService(ConsumerFunction<T> parse, String groupId, Class<T> type, Map<String, String> extraProperties) {
        this.parse = parse;
        this.consumer = new KafkaConsumer<>(getProperties(type, groupId, extraProperties));
    }

    void run() {
        while (true) {
            var records = consumer.poll(Duration.ofMillis(100));
            if(!records.isEmpty()) {
                System.out.println("Encontrei " + records.count() + " Registros");

                for (var record: records) {
                    try {
                        parse.consume(record);
                    } catch (Exception e) {
                        // only catches Exception because no matters witch Exception. I just want to recover and parse the next one.
                        // So far, just logging the Exception
                        e.printStackTrace();
                    }
                }
            }
        }
    }

    private Properties getProperties(Class<T> type, String groupId, Map<String, String> overrideProperties) {
        var properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, GsonDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        properties.putAll(overrideProperties);
        return properties;
    }

    @Override
    public void close() {
        consumer.close();
    }
}
