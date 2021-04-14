package org.deliveryfoodapp.ViewForUser;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.deliveryfoodapp.broker.TopicManager;

import javax.swing.*;
import java.util.concurrent.ExecutionException;

public class UserDataView {

    static private TopicManager topicManager;
    static private UserProducerShow userProducer;
    static private UserConsumer userConsumer;

    public static void showUserDataView(String nickname)
    {
        createsManagers(nickname);

        boolean done = false;
        ConsumerRecords<String, String> eventsOfNotify = null;

        while (!done)
        {
            userProducer.askItem(nickname,"");//Ask the items from the Server
            eventsOfNotify = userConsumer.readEventsOfNotify(); //Reads the record with all orders

            if (!eventsOfNotify.isEmpty())
                done = true;

        }

        JFrame frame = new JFrame();

        for (ConsumerRecord<String, String> record: eventsOfNotify) //Print the orders from the server
        {
            JOptionPane.showMessageDialog(frame,
                    record.key()+"\n" + record.value());

        }

        destroyManagers(nickname);
    }

    private static void createsManagers(String nickname)//Create producer, consumers and Topic Manager
    {
        topicManager = new TopicManager();
        userProducer = new UserProducerShow();
        userConsumer = new UserConsumer(nickname);

        try {
            topicManager.addTopicNotifyUser(nickname);
        } catch (ExecutionException e)
        {
            e.printStackTrace();
            System.out.println("Errore nel topic Manager");
            return;
        } catch (InterruptedException e) {
            e.printStackTrace();
            System.out.println("Errore nel topic Manager");
            return;
        }
    }

    private static void destroyManagers(String nickname)//Destroy producer, consumers and Topic Manager
    {
        userProducer.closeProducer();
        userConsumer.closeConsumer();
        try {
            topicManager.deleteTopicNotifyUser(nickname);
        } catch (ExecutionException e) {
            e.printStackTrace();
            System.out.println("Errore nel topic Manager");
            return;
        } catch (InterruptedException e) {
            e.printStackTrace();
            System.out.println("Errore nel topic Manager");
            return;
        }
    }
}
