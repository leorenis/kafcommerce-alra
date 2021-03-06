package br.com.alura.ecommerce;

import br.com.alura.ecommerce.consumer.KafkaService;
import br.com.alura.ecommerce.dispatcher.KafkaDispatcher;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import java.math.BigDecimal;
import java.util.Map;
import java.util.concurrent.ExecutionException;

public class FraudDetectorService {
    private final KafkaDispatcher<Order> orderKafkaDispatcher = new KafkaDispatcher<>();

    public static void main(String[] args) throws ExecutionException, InterruptedException {
        var fraudeService = new FraudDetectorService();
        try (var service = new KafkaService<>(FraudDetectorService.class.getSimpleName(), "ECOMMERCE_NEW_ORDER", fraudeService::parse, Map.of())) {
            service.run();
        }
    }

    private void parse(ConsumerRecord<String, Message<Order>> record) throws ExecutionException, InterruptedException {
        System.out.println("----------------------------------------");
        System.out.println("Processando new order cheking for fraud");
        System.out.println("Record Key: " +record.key());
        System.out.println("Record Value: " + record.value());
        System.out.println("Partition: " + record.partition());
        System.out.println("Offset: " + record.offset());
        try {
            Thread.sleep(2500);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        var message = record.value();
        var order = message.getPayload();
        CorrelationId correlationId = message.getId().continueWith(FraudDetectorService.class.getSimpleName());
        if (isFraud(order)) {
            // Pretending that the fraud happens when the amount is >= 4500
            orderKafkaDispatcher.send("ECOMMERCE_ORDER_REJECTED", order.getEmail(), correlationId, order);
            System.out.println("Order is a fraud!");
        } else
            orderKafkaDispatcher.send("ECOMMERCE_ORDER_APPROVED", order.getEmail(),correlationId, order);
    }

    private boolean isFraud(Order order) {
        return order.getAmount().compareTo(new BigDecimal("4500")) >= 0;
    }
}
