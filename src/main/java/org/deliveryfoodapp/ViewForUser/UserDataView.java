package org.deliveryfoodapp.ViewForUser;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.protocol.types.Field;
import org.deliveryfoodapp.broker.TopicManager;

import javax.swing.*;
import java.util.concurrent.ExecutionException;

public class UserDataView {

    static private TopicManager topicManager;
    static private UserProducerShow userProducer;
    static private UserConsumer userConsumer;

    public static void showUserDataView(String nickname)
    {
        JFrame frame = new JFrame();

        try {
            createsManagers(nickname);
        } catch (Exception e) {
            destroyManagers(nickname);
            JOptionPane.showMessageDialog(frame,
                    "Error of connection:Retry o check the connection");
            return;
        }

        String nick = "";
        String message = "";
        boolean done = false;
        ConsumerRecords<String, String> eventsOfNotify = null;

        while (!done)
        {
            userProducer.askUserData(nickname,"");//Ask the items from the Server
            eventsOfNotify = userConsumer.readEventsOfNotify(); //Reads the record with all orders

            if (!eventsOfNotify.isEmpty())
            {
                for (ConsumerRecord<String, String> record: eventsOfNotify) //Check the arrived records
                {
                    if (record.key().equals(nickname))//If there is the response message
                    {
                        nick = record.key();
                        message = record.value();
                        done = true;
                    }
                }
            }

        }

        destroyManagers(nickname);


        //Print the data of the user from the server
        JOptionPane.showMessageDialog(frame,
                    "Message"+"\n" + message);


    }

    public static void setAddressView(String nickname)
    {
        JFrame frame = new JFrame();

        try {
            createsManagers(nickname);
        } catch (Exception e) {
            destroyManagers(nickname);
            JOptionPane.showMessageDialog(frame,
                    "Error of connection:Retry o check the connection");
            return;
        }

        String nick = "";
        String message = "";
        boolean done = false;
        ConsumerRecords<String, String> eventsOfNotify = null;
        String address = InputReader.obtainAnAddress();

        while (!done)
        {
            userProducer.sendAddressCustomer(nickname,address);//Ask the items from the Server
            eventsOfNotify = userConsumer.readEventsOfNotify(); //Reads the record with all orders

            if (!eventsOfNotify.isEmpty())
            {
                for (ConsumerRecord<String, String> record: eventsOfNotify) //Check the arrived records
                {
                    if (record.key().equals(nickname))//If there is the response message
                    {
                        nick = record.key();
                        message = record.value();
                        done = true;
                    }
                }
            }

        }


        destroyManagers(nickname);

        JOptionPane.showMessageDialog(frame,
                "Message"+"\n" + message);
    }

    private static void createsManagers(String nickname)//Create producer, consumers and Topic Manager
    {
        userProducer = new UserProducerShow();
        userConsumer = new UserConsumer(nickname);
    }

    private static void destroyManagers(String nickname)//Destroy producer, consumers and Topic Manager
    {
        userProducer.closeProducer();
        userConsumer.closeConsumer();

    }
}
