package br.com.alura.ecommerce;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.function.IntConsumer;
import java.util.stream.IntStream;

public class NewOrderMain {
    public static void main(String[] args) {
        var producer = new KafkaProducer<String, String>(properties());
        IntStream.range(0, 10).forEach(producerCallback(producer));

    }

    private static IntConsumer producerCallback(KafkaProducer<String, String> producer) {
        return item -> {
            var key = UUID.randomUUID().toString();
            var value = "produto,98778";
            var record = new ProducerRecord<>("ECOMMERCE_NEW_ORDER", key, value);

            var email = "Thank you for you order! We are processing it.";
            var emailRecord = new ProducerRecord<>("ECOMMERCE_SEND_EMAIL", key, email);
            try {
                producer.send(record, getCallback()).get();
                producer.send(emailRecord, getCallback()).get();
            } catch (InterruptedException | ExecutionException e) {
                e.printStackTrace();
            }
        };
    }

    private static Callback getCallback() {
        return (data, ex) -> {
            if (ex != null) {
                ex.printStackTrace();
            }
            System.out.println("Sucesso no envio ao topico:  " + data.topic() + "::: partition" + data.partition() + " | offset: " + data.offset() + " :::timestamp " + data.timestamp());
        };
    }

    private static Properties properties() {
        var properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        return properties;
    }
}
