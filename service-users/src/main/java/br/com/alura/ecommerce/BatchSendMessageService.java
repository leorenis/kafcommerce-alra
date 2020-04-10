package br.com.alura.ecommerce;

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

    public static void main(String[] args) throws SQLException {
        var batchService = new BatchSendMessageService();
        try (var service = new KafkaService<>(BatchSendMessageService.class.getSimpleName(), "SEND_MESSAGE_TO_ALL_USERS", batchService::parse, String.class, Map.of())) {
            service.run();
        }
    }

    private void parse(ConsumerRecord<String, String> record) throws SQLException, ExecutionException, InterruptedException {
        System.out.println("----------------------------------------");
        System.out.println("Processando new batch generate report to all users");
        System.out.println("TOPIC: " + record.value());

        for (User user: getAllUsers()) {
            userDispatcher.send("USER_GENERATE_READING_REPORT", user.getUuid(), user);
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
