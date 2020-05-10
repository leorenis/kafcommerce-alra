package br.com.alura.ecommerce;

import br.com.alura.ecommerce.dispatcher.KafkaDispatcher;

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

    @Override
    public void destroy() {
        orderDispatcher.close();
    }

    @Override
    protected void doGet(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {

        // Get query parameters. We aren't caring about any security issues.
        var amount = new BigDecimal(req.getParameter("amount"));
        var emailAdress = req.getParameter("email");

        var orderId = UUID.randomUUID().toString();
        var correlationId = new CorrelationId(NewOrderServlet.class.getSimpleName());

        try {
            orderDispatcher.send("ECOMMERCE_NEW_ORDER", emailAdress, correlationId, new Order(orderId, amount, emailAdress));

            System.out.println("New order sent successfully.");
            resp.getWriter().println("New order sent!");
            resp.setStatus(HttpServletResponse.SC_OK);

        } catch (ExecutionException | InterruptedException e) {
            throw new ServletException(e);
        }
    }
}
