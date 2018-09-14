package com.flipkart.transact.yak.sep;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.After;
import org.junit.Before;

public class KafkaProducerTestCase {

    private Producer<byte[], byte[]> producer;

    @Before
    public void setup() throws FileNotFoundException, IOException {
        Properties props = new Properties();
        props.load(new FileInputStream(new File(
                "src/test/resources/kafka.properties")));
        this.producer = new KafkaProducer<>(props);
        System.out.println("producer setup done");
    }

//    @Test
    public void testPublish() throws InterruptedException, ExecutionException {
        ProducerRecord<byte[], byte[]> record = new ProducerRecord<byte[], byte[]>(
                "keyvalTest", "test".getBytes(), "data".getBytes());
        System.out.println("sending record");
        this.producer.send(record).get();
        System.out.println("sent record");
        System.out.println("done");
    }

    @After
    public void tearDown() {
        this.producer.close();
        System.out.println("producer closed");
    }
}
