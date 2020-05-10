package br.com.alura.ecommerce;

import br.com.alura.ecommerce.consumer.KafkaService;
import br.com.alura.ecommerce.dispatcher.KafkaDispatcher;
import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.util.Map;
import java.util.concurrent.ExecutionException;

public class EmailNewOrderService {
    private final KafkaDispatcher<Email> emailKafkaDispatcher = new KafkaDispatcher<>();

    public static void main(String[] args) throws ExecutionException, InterruptedException {
        var emailNewOrderService = new EmailNewOrderService();
        try (var service = new KafkaService<>(EmailNewOrderService.class.getSimpleName(), "ECOMMERCE_NEW_ORDER", emailNewOrderService::parse, Map.of())) {
            service.run();
        }
    }

    private void parse(ConsumerRecord<String, Message<Order>> record) {
        System.out.println("----------------------------------------");
        System.out.println("Preparing email new order.");
        var message = record.value();
        System.out.println("Record Value: " + message);

        var emailMessage = "Thank you for you order! We are processing it.";
        var order = message.getPayload();
        var correlationID = message.getId().continueWith(EmailNewOrderService.class.getSimpleName());

        emailKafkaDispatcher.sendAsync("ECOMMERCE_SEND_EMAIL", order.getEmail(),
                correlationID, new Email("Email new Order Service", emailMessage));

    }

}
