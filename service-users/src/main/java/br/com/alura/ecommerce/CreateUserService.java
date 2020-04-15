package br.com.alura.ecommerce;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Map;
import java.util.UUID;

public class CreateUserService {

    private final Connection connection;

    CreateUserService() throws SQLException {
        String url = "jdbc:sqlite:service-users/target/users_database.db";
        connection = DriverManager.getConnection(url);
        connection.createStatement().execute("create table if not exists Users(uuid varchar(200) primary key, email varchar(200))");
    }

    public static void main(String[] args) throws SQLException {
        var userService = new CreateUserService();
        try (var service = new KafkaService<>(CreateUserService.class.getSimpleName(), "ECOMMERCE_NEW_ORDER", userService::parse, Order.class, Map.of())) {
            service.run();
        }
    }

    private void parse(ConsumerRecord<String, Message<Order>> record) throws SQLException {
        System.out.println("----------------------------------------");
        System.out.println("Processando new order cheking for new user");
        System.out.println("Record Value: " + record.value());

        var message = record.value();
        var order = message.getPayload();
        if(isNewUser(order.getEmail())){
            insertNewUser(order.getEmail());
        }
    }

    private void insertNewUser(String email) throws SQLException {
        var insertStatement = connection.prepareStatement("insert into Users (uuid, email) values (?, ?)");
        insertStatement.setString(1, UUID.randomUUID().toString());
        insertStatement.setString(2, email);
        insertStatement.execute();
        System.out.println("Usuario com email: "+email+" adicionado.");
    }

    private boolean isNewUser(String email) throws SQLException {
        var exists = connection.prepareStatement("select uuid from Users where email = ? limit 1");
        exists.setString(1, email);
        var results = exists.executeQuery();
        return !results.next();
    }
}
