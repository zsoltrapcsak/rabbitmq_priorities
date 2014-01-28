

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.QueueingConsumer;

import java.io.FileInputStream;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.regex.Pattern;

public class Receiver {


    public static void main(String[] argv) throws Exception {
        Properties prop = new Properties();
        prop.load(ClassLoader.getSystemResourceAsStream("config.properties"));

        String rabbitHost = prop.getProperty("rmq-uri");
        String rabbitUser = prop.getProperty("rmq-user");
        String rabbitPassword = prop.getProperty("rmq-password");
        String rabbitQueue = prop.getProperty("rmq-queue");

        Filters filters = new Filters();
        filters.setReQueue(true);
        filters.setMessageNumber(1);
        filters.setExpectedPriority(5);
        //filters.setLocateMessageByData("[\\s\\S]*2014-01-16/PVWF20130[\\s\\S]*");

        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost(rabbitHost);
        factory.setUsername(rabbitUser);
        factory.setPassword(rabbitPassword);
        Connection connection = factory.newConnection();
        Channel channel = connection.createChannel();
        Channel channel2 = connection.createChannel();

        QueueingConsumer consumer = new QueueingConsumer(channel);
        QueueingConsumer consumer2 = new QueueingConsumer(channel2);

        Map<String, Object> args = new HashMap<String, Object>();
        args.put("x-priority", 1);

        Map<String, Object> args2 = new HashMap<String, Object>();
        args2.put("x-priority", 0);

        channel.basicConsume(rabbitQueue, false, "", false, false, args, consumer);
        channel2.basicConsume(rabbitQueue, false, "", false, false, args2, consumer2);

        Thread one = new TestThread(consumer, channel, 1);
        Thread two = new TestThread(consumer2, channel2, 2);
        one.start();
        two.start();
    }
}

class TestThread extends Thread {
    private QueueingConsumer consumer;
    private int number;
    private Channel channel;

    public TestThread(QueueingConsumer consumer, Channel channel, int number){
        this.consumer = consumer;
        this.number = number;
        this.channel = channel;
    }
    public void run() {
        int i = 0;
        int priority0 = 0;
        int priority1 = 0;
        System.out.println("Started thread: " + number);
        while(true) {
            try {
                QueueingConsumer.Delivery delivery = consumer.nextDelivery();
                channel.basicAck(delivery.getEnvelope().getDeliveryTag(), false);
                if(delivery.getProperties().getPriority() == 0) priority0++;
                else if(delivery.getProperties().getPriority() == 1) priority1++;
                else System.out.println("------------------------------- wrong priority: " + delivery.getProperties().getPriority());
                i++;
                System.out.println("message arrived: " + number + " index: " + i + " priority number for 0 is: " + priority0 + " for 1: " + priority1);

                //if we want to simulate some business work...
//                Thread.sleep(100);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }


    }
}

class Filters {
    private int messageNumber = 1;
    private boolean reQueue;
    private int expectedPriority = -1;
    private String locateMessageByData;

    @Override
    public String toString() {
        return "Filters{" +
                "messageNumber=" + messageNumber +
                ", reQueue=" + reQueue +
                ", expectedPriority=" + expectedPriority +
                ", locateMessageByData='" + locateMessageByData + '\'' +
                '}';
    }

    public int getMessageNumber() {
        return messageNumber;
    }

    public void setMessageNumber(int messageNumber) {
        this.messageNumber = messageNumber;
    }

    public boolean isReQueue() {
        return reQueue;
    }

    public void setReQueue(boolean reQueue) {
        this.reQueue = reQueue;
    }

    public int getExpectedPriority() {
        return expectedPriority;
    }

    public void setExpectedPriority(int expectedPriority) {
        this.expectedPriority = expectedPriority;
    }

    public String getLocateMessageByData() {
        return locateMessageByData;
    }

    public void setLocateMessageByData(String locateMessageByData) {
        this.locateMessageByData = locateMessageByData;
    }
}