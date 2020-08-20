package com.github.maven.tutorial;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

public class ConsumerDemoWithThread {

    public static void main(String[] args) {
        new ConsumerDemoWithThread().run();
    }
    public ConsumerDemoWithThread(){

    }
    public  void run(){
        CountDownLatch latch = new CountDownLatch(1);
        Runnable myConsumerThread = new ConsumerThread(latch);
    }


}
public class ConsumerThread implements Runnable {
    private CountDownLatch latch;
    private KafkaConsumer<String, String> consumer;
    Logger logger = LoggerFactory.getLogger(ConsumerDemo.class.getName());
    public ConsumerThread(CountDownLatch latch) {
        this.latch = latch;

        Properties properties = new Properties();
        String groupid = "my-fifth-application";
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupid);
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        consumer = new KafkaConsumer<String, String>((properties));
    }

    @Override
    public void run() {
        try {
            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
                for (ConsumerRecord<String, String> record : records) {
                    logger.info("Key:" + record.key() + " Value:" + record.value());
                    logger.info("Partition:" + record.partition() + "Offset:" + record.offset());
                }
            }
        }catch (WakeupException e){
            logger.info("Received shutdown");
        }
        finally {
            consumer.close();
            latch.countDown();
        }
    }
    public void shutdown(){
        consumer.wakeup();
    }

}