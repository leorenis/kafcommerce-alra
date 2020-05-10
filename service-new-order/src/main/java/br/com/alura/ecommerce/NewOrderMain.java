package br.com.alura.ecommerce;

import br.com.alura.ecommerce.dispatcher.KafkaDispatcher;
import java.math.BigDecimal;
import java.util.UUID;
import java.util.stream.IntStream;

public class NewOrderMain {
    public static void main(String[] args) {
        try (var orderDispatcher = new KafkaDispatcher<Order>()) {
            IntStream.range(0, 10).forEach( item -> {
                var orderId = UUID.randomUUID().toString();
                var amount = BigDecimal.valueOf(Math.random() * 5000 + 1);
                var emailAdress = orderId+"@gmail.com";
                var correlationId = new CorrelationId(NewOrderMain.class.getSimpleName());
                orderDispatcher.sendAsync("ECOMMERCE_NEW_ORDER", emailAdress,correlationId, new Order(orderId, amount, emailAdress));
            });
        }
    }
}
