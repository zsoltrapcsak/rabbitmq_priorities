import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.Channel;

import java.io.FileInputStream;
import java.util.Properties;

public class Sender {

//    private final static String QUEUE_NAME = "hello";

    public static void main(String[] argv) throws Exception {

        Properties prop = new Properties();
        prop.load(new FileInputStream("c:/picsolve/RabbitMQ-Client/src/config.properties"));

        String rabbitHost = prop.getProperty("rmq-uri");
        String rabbitUser = prop.getProperty("rmq-user");
        String rabbitPassword = prop.getProperty("rmq-password");
        String rabbitExchange = prop.getProperty("rmq-exchange");
        String rabbitRoutingKey = prop.getProperty("rmq-routingkey");

        String messageData = "testData3";
        String priority = "0";

        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost(rabbitHost);
        factory.setUsername(rabbitUser);
        factory.setPassword(rabbitPassword);
        Connection connection = null;
        try{
            connection = factory.newConnection();
        }
        catch(Exception e){
            e.printStackTrace();
        }
        Channel channel = connection.createChannel();

        String routingKey = rabbitRoutingKey;
        String message = messageData;

        SenderThread senderThread00 = new SenderThread(channel, rabbitExchange, routingKey, "0");
//        SenderThread senderThread01 = new SenderThread(channel, rabbitExchange, routingKey, "0");
//        SenderThread senderThread02 = new SenderThread(channel, rabbitExchange, routingKey, "0");
//        SenderThread senderThread03 = new SenderThread(channel, rabbitExchange, routingKey, "0");
//        SenderThread senderThread04 = new SenderThread(channel, rabbitExchange, routingKey, "0");
        SenderThread senderThread2 = new SenderThread(channel, rabbitExchange, routingKey, "1");
        senderThread00.start();
//        senderThread01.start();
//        senderThread02.start();
//        senderThread03.start();
//        senderThread04.start();
        senderThread2.start();
    }
}

class SenderThread extends Thread {

    private Channel channel;
    private String rabbitExchange;
    private String routingKey;
    private String priority;

    public SenderThread(Channel channel, String rabbitExchange, String routingKey, String priority) {
        this.channel = channel;
        this.rabbitExchange = rabbitExchange;
        this.routingKey = routingKey;
        this.priority = priority;
    }

    public void run() {
        String message = "testData4";
        try {
            for(int i = 0; i < 1000000; i++) {
                channel.basicPublish(rabbitExchange, routingKey, new AMQP.BasicProperties.Builder()
                        .contentType("text/plain").deliveryMode(2)
                        .priority(Integer.valueOf(priority)).build(), message.getBytes());
                //System.out.println(" [x] Sent '" + routingKey + "':'" + message + "'");
            }
        } catch(Exception e) {
            e.printStackTrace();
        }
    }
}
