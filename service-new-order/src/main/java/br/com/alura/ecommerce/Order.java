package br.com.alura.ecommerce;

import java.math.BigDecimal;

class Order {
    private final String userId, orderId, email;
    private final BigDecimal amount;

    Order(String userId, String orderId, BigDecimal amount, String email) {
        this.userId = userId;
        this.orderId = orderId;
        this.amount = amount;
        this.email = email;
    }
}