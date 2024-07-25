package org.acme.adapters.input;

import io.quarkus.runtime.ShutdownEvent;
import io.quarkus.runtime.StartupEvent;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.event.Observes;
import jakarta.inject.Inject;
import jakarta.jms.Connection;
import jakarta.jms.ConnectionFactory;
import jakarta.jms.JMSContext;
import jakarta.jms.JMSException;
import jakarta.jms.Message;
import jakarta.jms.MessageConsumer;
import jakarta.jms.MessageProducer;
import jakarta.jms.Queue;
import jakarta.jms.Session;
import jakarta.jms.TextMessage;
import java.util.Enumeration;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Logger;
import org.eclipse.microprofile.config.inject.ConfigProperty;

@ApplicationScoped
public class JmsMessageConsumer {

   private static final Logger log = Logger.getLogger(JmsMessageConsumer.class.getName());

   @Inject
   ConnectionFactory connectionFactory;

   private JMSContext context;
   private Connection connection;
   private Session session;
   private MessageConsumer consumer;
   private MessageProducer dlqProducer;

   @ConfigProperty(name = "my.edu.queue.name", defaultValue = "my.edu.queue")
   String queueName;

   private AtomicInteger counter = new AtomicInteger(0);


   void onStart(@Observes StartupEvent ev) throws JMSException {
      connection = connectionFactory.createConnection();
      session = connection.createSession(true, Session.SESSION_TRANSACTED);

      Queue queue = session.createQueue(queueName);
      consumer = session.createConsumer(queue);

      Queue dlq = session.createQueue(queueName + ".dlq");
      dlqProducer = session.createProducer(dlq);

      connection.start();

      receiveMessages();
   }

   void onStop(@Observes ShutdownEvent ev) throws JMSException {
      connection.close();
   }

   private void receiveMessages() {
      try {
         consumer.setMessageListener(message -> {
            try {
               processMessage(message.getBody(String.class));
               session.commit();
            } catch (Exception e) {
               log.severe("Error processing message: %s".formatted(e.getMessage()));
               try {
                  // sendToDLQ(message);
                  session.rollback();
               } catch (JMSException ex) {
                  throw new RuntimeException(ex);
               }
            }
         });
      } catch (JMSException e) {
         log.severe("Error setting message listener: %s".formatted(e.getMessage()));
      }
   }

   private void processMessage(String text) {
      counter.incrementAndGet();
      if (counter.get() % 5 == 0) {
         throw new RuntimeException("Error in business logic");
      }
      log.info("Processed message: " + text);
   }

   private void sendToDLQ(Message message) {
      try {
         TextMessage dlqMessage = session.createTextMessage(message.getBody(String.class));

         Enumeration propertyNames = message.getPropertyNames();
         while (propertyNames.hasMoreElements()) {
            Object elem = propertyNames.nextElement();
            dlqMessage.setObjectProperty(elem.toString(), message.getObjectProperty(elem.toString()));
         }

         dlqMessage.setStringProperty("_AMQ_ORIG_ADDRESS", queueName);
         dlqMessage.setStringProperty("_AMQ_ORIG_QUEUE", queueName);
         dlqProducer.send(dlqMessage);
         session.commit();
         log.info("Message sent to DLQ");
      } catch (JMSException e) {
         log.severe("Error sending message to DLQ: %s".formatted(e.getMessage()));
      }
   }

}
