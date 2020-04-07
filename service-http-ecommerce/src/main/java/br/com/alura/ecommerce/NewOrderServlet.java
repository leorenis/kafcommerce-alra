package br.com.alura.ecommerce;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.math.BigDecimal;
import java.util.UUID;
import java.util.concurrent.ExecutionException;

public class NewOrderServlet extends HttpServlet {

    private final KafkaDispatcher<Order> orderDispatcher = new KafkaDispatcher<>();
    private final KafkaDispatcher<Email> emailDispatcher = new KafkaDispatcher<>();

    @Override
    public void destroy() {
        orderDispatcher.close();
        emailDispatcher.close();
    }

    @Override
    protected void doGet(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {

        // Get query parameters. We aren't caring about any security issues.
        var amount = new BigDecimal(req.getParameter("amount"));
        var emailAdress = req.getParameter("email");

        var orderId = UUID.randomUUID().toString();
        var emailMessage = "Thank you for you order! We are processing it.";

        try {
            orderDispatcher.send("ECOMMERCE_NEW_ORDER", emailAdress, new Order(orderId, amount, emailAdress));
            emailDispatcher.send("ECOMMERCE_SEND_EMAIL", emailAdress, new Email("New email", emailMessage));

            System.out.println("New order sent successfully.");
            resp.getWriter().println("New order sent!");
            resp.setStatus(HttpServletResponse.SC_OK);

        } catch (ExecutionException | InterruptedException e) {
            throw new ServletException(e);
        }
    }
}
