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

   // private final ExecutorService scheduler = Executors.newSingleThreadExecutor();

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


   /*

   2nd Version
   void onStart(@Observes StartupEvent ev) {
      context = connectionFactory.createContext(JMSContext.SESSION_TRANSACTED);
      JMSConsumer consumer = context.createConsumer(context.createQueue(queueName));
      consumer.setMessageListener(this::onMessage);
   }

   void onStop(@Observes ShutdownEvent ev) {
      // scheduler.shutdown();
   }

   private void onMessage(Message message) {
      try {
         String text = message.getBody(String.class);
         processMessage(text);
         message.acknowledge();
      } catch (Exception e) {
         context.rollback();
         log.severe("Error processing message: %s".formatted(e.getMessage()));
         throw new RuntimeException("Failed to process message", e);
      }
   }

   private void processMessage(String text) {
      // Your business logic here
      counter.incrementAndGet();
      if (counter.get() % 5 == 0) {
         throw new RuntimeException("Error in business logic");
      }
      log.info("Processed message: " + text);
   }

    */

   /*

   1st Version
   @Override
   public void run() {
      while (true) {
         try (JMSContext context = connectionFactory.createContext(JMSContext.CLIENT_ACKNOWLEDGE);
              JMSConsumer consumer = context.createConsumer(context.createQueue(queueName))) {
            log.info("Waiting to receive!");
            Message message = consumer.receive();
            if (message != null) {
               processMessage(message, context);
            }
         }
      }
   }

   private void processMessage(Message message, JMSContext context) {
      try {
         if (message instanceof TextMessage) {
            int ii = counter.incrementAndGet();
            String text = ((TextMessage) message).getText();
            if (ii % 5 == 0) {

               log.info("Send msg to DLQ[%d]".formatted(counter.get()));
               // context.recover();

               String dlqQueueName = queueName + ".dlq";
               try(JMSContext producerContext = connectionFactory.createContext(JMSContext.AUTO_ACKNOWLEDGE)) {
                  JMSProducer producer =
                      producerContext.createProducer().send(producerContext.createQueue(dlqQueueName),
                                                            message);
               }

               return;
            }
            log.info("Acknowledge msg[%d]".formatted(counter.get()));
            // message.acknowledge();
         }
      } catch (Exception e) {
         log.severe(e.getMessage());
      }
   }
    */

   /*
   private void sendToDLQ(JMSContext context, Queue dlq, Message originalMessage) {
      try {
         Message dlqMessage = context.createTextMessage(((TextMessage) originalMessage).getText());
         // Copy properties and headers from original message
         for (String propertyName : Collections.list(originalMessage.getPropertyNames())) {
            dlqMessage.setObjectProperty(propertyName, originalMessage.getObjectProperty(propertyName));
         }
         dlqMessage.setJMSCorrelationID(originalMessage.getJMSMessageID());
         dlqMessage.setStringProperty("OriginalQueue", queueName);
         dlqMessage.setStringProperty("ErrorMessage", "Failed to process in business logic");

         JMSProducer producer = context.createProducer();
         producer.send(dlq, dlqMessage);
         log.info("Message sent to DLQ");

         // Acknowledge the original message after sending to DLQ
         originalMessage.acknowledge();
      } catch (JMSException e) {
         log.errorf("Error sending message to DLQ: %s", e.getMessage());
      }
   }

    */

}
