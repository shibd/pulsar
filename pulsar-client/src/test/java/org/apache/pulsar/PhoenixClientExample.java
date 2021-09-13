package org.apache.pulsar;

import lombok.Cleanup;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;

/**
 * @author baozi
 */
public class PhoenixClientExample {

    public static void main(String[] args) throws PulsarClientException {

        String serviceUrl = "pulsar://localhost:6650";

        @Cleanup
        PulsarClient client = PulsarClient.builder()
                .serviceUrl(serviceUrl)
                .build();


        Consumer<byte[]> consumer = client.newConsumer().topic("my-topic").subscriptionName("my-subscribe").subscribe();

        while (true) {
            // Wait for a message
            Message msg = consumer.receive();

            try {
                // Do something with the message
                System.out.println("Message received: " + new String(msg.getData()));

                // Acknowledge the message so that it can be deleted by the message broker
//                consumer.acknowledge(msg);
            } catch (Exception e) {
                // Message failed to process, redeliver later
                consumer.negativeAcknowledge(msg);
            }
        }
    }
}
