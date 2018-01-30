package com.chida.jms;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.broker.BrokerService;

import javax.jms.Connection;
import javax.jms.DeliveryMode;
import javax.jms.Destination;
import javax.jms.ExceptionListener;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Session;
import javax.jms.TextMessage;
import java.util.concurrent.TimeUnit;

public class App1 {

    private static String BROKER_URL = "tcp://127.0.0.1:61617";

    public static void main(String[] args) throws Exception {
        App1 app = new App1();

        if(app.startBroker().isStarted()){
            System.out.println("Broker service started");
        }


        thread(new App1.HelloWorldProducer(), false);
        Thread.sleep(1000);
        thread(new App1.HelloWorldConsumer("client 1"), false);
        thread(new App1.HelloWorldConsumer("client 2"), false);


    }

    public static void thread(Runnable runnable, boolean daemon) {
        Thread brokerThread = new Thread(runnable);
        brokerThread.setDaemon(daemon);
        brokerThread.start();
    }

    public static class HelloWorldProducer implements Runnable {
        public void run() {
            try {
                // Create a ConnectionFactory
                ActiveMQConnectionFactory connectionFactory = new ActiveMQConnectionFactory(BROKER_URL);

                // Create a Connection
                Connection connection = connectionFactory.createConnection();
                connection.start();

                // Create a Session
                Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);

                // Create the destination (Topic or Queue)
                Destination destination = session.createTopic("TEST.FOO");

                // Create a MessageProducer from the Session to the Topic or Queue
                MessageProducer producer = session.createProducer(destination);
                producer.setDeliveryMode(DeliveryMode.NON_PERSISTENT);

                // Create a messages

                try {
                    while(true){
                        String text = "Hello world! From: " + Math.random();
                        TextMessage message = session.createTextMessage(text);

                        // Tell the producer to send the message
                        System.out.println("Sent message: "+ text);
                        producer.send(message);

                        TimeUnit.SECONDS.sleep(5);
                    }
                }finally {
                    // Clean up
                    producer.close();
                    session.close();
                    connection.close();
                }




            }
            catch (Exception e) {
                System.out.println("Caught: " + e);
                e.printStackTrace();
            }
        }
    }

    public static class HelloWorldConsumer implements Runnable, ExceptionListener {
        private String name;

        public HelloWorldConsumer(String name){
            this.name = name;
        }
        public void run() {
            try {

                // Create a ConnectionFactory
                ActiveMQConnectionFactory connectionFactory = new ActiveMQConnectionFactory(BROKER_URL);

                // Create a Connection
                Connection connection = connectionFactory.createConnection();
                connection.start();

                connection.setExceptionListener(this);

                // Create a Session
                Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);

                // Create the destination (Topic or Queue)
                Destination destination = session.createTopic("TEST.FOO");

                // Create a MessageConsumer from the Session to the Topic or Queue
                MessageConsumer consumer = session.createConsumer(destination);

                // Wait for a message
                try {
                    while(true){
                        Message message = consumer.receive();

                        if (message instanceof TextMessage) {
                            TextMessage textMessage = (TextMessage) message;
                            String text = textMessage.getText();
                            System.out.println("Received on  " +this.name + " : " + text);
                        } else {
                            System.out.println("Received on  " +this.name + " : " + message);
                        }
                    }
                }finally {
                    consumer.close();
                    session.close();
                    connection.close();
                }



            } catch (Exception e) {
                System.out.println("Caught: " + e);
                e.printStackTrace();
            }
        }

        public synchronized void onException(JMSException ex) {
            System.out.println("JMS Exception occured.  Shutting down client.");
        }
    }

    public BrokerService startBroker(){
        BrokerService brokerService = new BrokerService();

        try {
            brokerService.addConnector(BROKER_URL);
            brokerService.start();
        } catch (Exception e) {
            e.printStackTrace();
        }
        return brokerService;
    }
}
