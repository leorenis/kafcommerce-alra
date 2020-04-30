package br.com.alura.ecommerce;

import br.com.alura.ecommerce.consumer.KafkaService;
import br.com.alura.ecommerce.dispatcher.KafkaDispatcher;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;

public class BatchSendMessageService {

    private final Connection connection;
    private final KafkaDispatcher<User> userDispatcher = new KafkaDispatcher<User>();

    BatchSendMessageService() throws SQLException {
        String url = "jdbc:sqlite:service-users/target/users_database.db";
        connection = DriverManager.getConnection(url);
        connection.createStatement().execute("create table if not exists Users(uuid varchar(200) primary key, email varchar(200))");
    }

    public static void main(String[] args) throws SQLException, ExecutionException, InterruptedException {
        var batchService = new BatchSendMessageService();
        try (var service = new KafkaService<>(BatchSendMessageService.class.getSimpleName(), "ECOMMERCE_SEND_MESSAGE_TO_ALL_USERS", batchService::parse, Map.of())) {
            service.run();
        }
    }

    private void parse(ConsumerRecord<String, Message<String>> record) throws SQLException, ExecutionException, InterruptedException {
        System.out.println("----------------------------------------");
        System.out.println("Processing new batch generate report to all users");
        var message = record.value();
        System.out.println("TOPIC: " + message.getPayload());

        for (User user: getAllUsers()) {
            userDispatcher.sendAsync(message.getPayload(), user.getUuid(),
                message.getId().continueWith(BatchSendMessageService.class.getSimpleName()), user);
            System.out.println("Acho que enviei para o user " + user);
        }
    }

    private List<User> getAllUsers() throws SQLException {
        var r = connection.prepareStatement("select uuid, email from Users").executeQuery();
        var users = new ArrayList<User>();
        while (r.next()) {
            users.add(new User(r.getString(1), r.getString(2)));
        }
        return users;
    }
}
