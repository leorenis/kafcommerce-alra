package br.com.alura.ecommerce;

import java.math.BigDecimal;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.stream.IntStream;

public class NewOrderMain {
    public static void main(String[] args) {
        try (var orderDispatcher = new KafkaDispatcher<Order>()) {
            try (var emailDispatcher = new KafkaDispatcher<Email>()) {

                IntStream.range(0, 10).forEach( item -> {
                    var orderId = UUID.randomUUID().toString();
                    var amount = BigDecimal.valueOf(Math.random() * 5000 + 1);
                    var emailMessage = "Thank you for you order! We are processing it.";
                    var emailAdress = orderId+"@gmail.com";
                    try {
                        orderDispatcher.send("ECOMMERCE_NEW_ORDER", emailAdress, new Order(orderId, amount, emailAdress));
                        emailDispatcher.send("ECOMMERCE_SEND_EMAIL", emailAdress, new Email("New email", emailMessage));
                    } catch (ExecutionException | InterruptedException e) {
                        e.printStackTrace();
                    }
                });
            }
        }
    }
}
