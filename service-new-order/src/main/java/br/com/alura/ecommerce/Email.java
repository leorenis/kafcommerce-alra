package br.com.alura.ecommerce;

public class Email {
    private final String subject;
    private final String body;

    public Email(String subject, String body) {
        this.subject = subject;
        this.body = body;
    }

    @Override
    public String toString() {
        return "Email with: subject='" + subject + ", body='" + body;
    }
}
