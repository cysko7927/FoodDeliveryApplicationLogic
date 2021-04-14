package org.deliveryfoodapp.broker;

import org.apache.kafka.clients.admin.*;

import java.util.Collections;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ExecutionException;

/**
 * This class allows to insert all the topics required for the application inside the broker server
 */
public class TopicManager {

    private static final String defaultTopicName = "UserRegistration"; //nome topic d'aggiungere
    private static final int defaultTopicPartitions = 2; //Partizioni che deve avere il topic
    private static final short defaultReplicationFactor = 1; //Replication factor

    private static final String serverAddr = "localhost:9092";

    public static void main(String[] args) throws Exception {
        final String topicName =  NameOfTopics.showOrder;
        final int topicPartitions =  defaultTopicPartitions;
        final short replicationFactor =  defaultReplicationFactor;

        Properties props = new Properties();
        props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, serverAddr);
        AdminClient adminClient = AdminClient.create(props); //Creo l'oggetto che comunica con il server per aggiungere o eliminare Topics

        ListTopicsResult listResult = adminClient.listTopics();
        Set<String> topicsNames = listResult.names().get();
        System.out.println("Available topics: " + topicsNames);

        if (topicsNames.contains(topicName)) {
            System.out.println("Deleting topic " + topicName);
            DeleteTopicsResult delResult = adminClient.deleteTopics(Collections.singletonList(topicName));
            delResult.all().get();
            System.out.println("Done!");
            // Wait for the deletion
            Thread.sleep(5000);
        }

        System.out.println("Adding topic " + topicName + " with " + topicPartitions + " partitions");
        NewTopic newTopic = new NewTopic(topicName, topicPartitions, replicationFactor);
        CreateTopicsResult createResult = adminClient.createTopics(Collections.singletonList(newTopic));
        createResult.all().get();
        System.out.println("Done!");
    }

    public void addTopicNotifyUser(String nickname) throws ExecutionException, InterruptedException {
        final String topicName =  NameOfTopics.notifyUser +nickname;
        final int topicPartitions =  defaultTopicPartitions;
        final short replicationFactor =  defaultReplicationFactor;

        Properties props = new Properties();
        props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, serverAddr);
        AdminClient adminClient = AdminClient.create(props); //Creo l'oggetto che comunica con il server per aggiungere o eliminare Topics

        ListTopicsResult listResult = adminClient.listTopics();
        Set<String> topicsNames = listResult.names().get();
        System.out.println("Available topics: " + topicsNames);

        if (topicsNames.contains(topicName)) {
            System.out.println("Deleting topic " + topicName);
            DeleteTopicsResult delResult = adminClient.deleteTopics(Collections.singletonList(topicName));
            delResult.all().get();
            System.out.println("Done!");
            // Wait for the deletion
            Thread.sleep(5000);
        }

        System.out.println("Adding topic " + topicName + " with " + topicPartitions + " partitions");
        NewTopic newTopic = new NewTopic(topicName, topicPartitions, replicationFactor);
        CreateTopicsResult createResult = adminClient.createTopics(Collections.singletonList(newTopic));
        createResult.all().get();
        System.out.println("Done!");
    }

    public void deleteTopicNotifyUser(String nickname) throws ExecutionException, InterruptedException
    {
        final String topicName =  NameOfTopics.notifyUser +nickname;
        final int topicPartitions =  defaultTopicPartitions;
        final short replicationFactor =  defaultReplicationFactor;

        Properties props = new Properties();
        props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, serverAddr);
        AdminClient adminClient = AdminClient.create(props); //Creo l'oggetto che comunica con il server per aggiungere o eliminare Topics

        ListTopicsResult listResult = adminClient.listTopics();
        Set<String> topicsNames = listResult.names().get();
        System.out.println("Available topics: " + topicsNames);

        if (topicsNames.contains(topicName)) {
            System.out.println("Deleting topic " + topicName);
            DeleteTopicsResult delResult = adminClient.deleteTopics(Collections.singletonList(topicName));
            delResult.all().get();
            System.out.println("Done!");
            // Wait for the deletion
            Thread.sleep(5000);

            listResult = adminClient.listTopics();
            topicsNames = listResult.names().get();
            System.out.println("Available topics: " + topicsNames);
        }

        adminClient.close();


    }


}
