package br.com.alura.ecommerce;

import java.math.BigDecimal;

class Order {
    private final String orderId, email;
    private final BigDecimal amount;

    Order(String orderId, BigDecimal amount, String email) {
        this.orderId = orderId;
        this.amount = amount;
        this.email = email;
    }

    public String getEmail() {
        return email;
    }
}
