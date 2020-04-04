package br.com.alura.ecommerce;

import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.function.IntConsumer;
import java.util.stream.IntStream;

public class NewOrderMain {
    public static void main(String[] args) {
        try (var dispatcher = new KafkaDispatcher()) {
            IntStream.range(0, 10).forEach(producerCallback(dispatcher));
        }
    }

    private static IntConsumer producerCallback(KafkaDispatcher dispatcher) {
        return item -> {
            var key = UUID.randomUUID().toString();
            var produto = "produto," + key;
            var email = "Thank you for you order! We are processing it.";
            try {
                dispatcher.send("ECOMMERCE_NEW_ORDER", key, produto);
                dispatcher.send("ECOMMERCE_SEND_EMAIL", key, email);
            } catch (ExecutionException | InterruptedException e) {
                e.printStackTrace();
            }
        };
    }
}
