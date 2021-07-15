package org.deliveryfoodapp.Microservices.userServices;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.deliveryfoodapp.broker.NameOfTopics;
import org.deliveryfoodapp.broker.NetworkBroker;

import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Properties;

/**
 * Consumers that reads the events about user-Registration from the kafka Broker
 */
public class ConsumerEventsForUser
{
    private static final String defaultGroupId = "groupA";

    private static final String serverAddr = "localhost:9092";
    private static final boolean autoCommit = false;

    // Default is "latest": try "earliest" instead
    private static final String offsetResetStrategy = "latest";

    Properties props;
    String groupId;
    String topic;
    KafkaConsumer<String, String> consumer;

    /**
     * Create the consumer for Registration Event and subscribe it to the Broker
     */
    public ConsumerEventsForUser()
    {
        this.groupId =  defaultGroupId;


        this.props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, NetworkBroker.server0 + "," + NetworkBroker.server1+ "," + NetworkBroker.server2);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        //props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, String.valueOf(autoCommit));

        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, offsetResetStrategy);

        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());

        List<String> topics = new ArrayList<>();
        topics.add(NameOfTopics.userRegistration);
        topics.add(NameOfTopics.showUserData);
        topics.add(NameOfTopics.updateAddressShippingUser);
        this.consumer = new KafkaConsumer<>(props);
        consumer.subscribe(topics);
    }

    /**
     * Reads a bit of events about user Registration from the Broker
     * @return The list of Events Available
     */
    ConsumerRecords<String, String>  readEventsOfRegistration()
    {
        return consumer.poll(Duration.of(5, ChronoUnit.SECONDS));
    }

    void commitState()
    {
        consumer.commitSync();
    }

    void closeConsumer()
    {
        consumer.close();
    }
}
