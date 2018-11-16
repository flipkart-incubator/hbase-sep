package com.flipkart.yak.sep;

/*
 * Created by Amanraj on 09/08/18 .
 */

import kafka.consumer.Consumer;
import kafka.consumer.ConsumerConfig;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.*;

@SuppressWarnings("WeakerAccess")
public class KafkaConsumer {

    public static List<byte[]> readMessages(String topicName, final int expectedMessages) throws TimeoutException {

        Properties consumerProperties = new Properties();
        consumerProperties.put("zookeeper.connect", "localhost:9002");
        consumerProperties.put("group.id", "10");
        consumerProperties.put("socket.timeout.ms", "500");
        consumerProperties.put("consumer.id", "test");
        consumerProperties.put("auto.offset.reset", "smallest");
        consumerProperties.put("key.deserializer", "org.apache.kafka.common.serialization.ByteArrayDeserializer");
        consumerProperties.put("value.deserializer", "org.apache.kafka.common.serialization.ByteArrayDeserializer");

        ExecutorService executorService = Executors.newSingleThreadExecutor();
        ConsumerConnector javaConsumerConnector = Consumer
                .createJavaConsumerConnector(new ConsumerConfig(consumerProperties));

        final KafkaStream kafkaStreams = javaConsumerConnector
                .createMessageStreams(Collections.singletonMap(topicName, 1))
                .get(topicName).get(0);

        Future<List<byte[]>> future = executorService.submit(() -> {
            List<byte[]> messages = new ArrayList<>();


            ConsumerIterator iterator = kafkaStreams.iterator();

            while (messages.size() != expectedMessages && iterator.hasNext()) {
                byte[] message = (byte[]) iterator.next().message();
                messages.add(message);
            }

            return messages;
        });

        try {
            return future.get(5L, TimeUnit.SECONDS);
        } catch (ExecutionException | TimeoutException | InterruptedException var16) {
            throw new TimeoutException("Timed out waiting for messages");
        } finally {
            executorService.shutdown();
            javaConsumerConnector.shutdown();

        }

    }
}
