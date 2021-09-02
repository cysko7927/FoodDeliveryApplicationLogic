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

        //Creates the Producer and Consumer to interact with the brokers of kafka
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
            userProducer.askUserData(nickname,"");//Ask the data from the Server
            eventsOfNotify = userConsumer.readEventsOfNotify(); //Reads the record with the data of the user

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

        //Closes the Producer and Consumer to interact with the brokers of kafka
        destroyManagers(nickname);


        //Print the data of the user from the server
        JOptionPane.showMessageDialog(frame,
                    "Message"+"\n" + message);


    }

    public static void setAddressView(String nickname)
    {
        JFrame frame = new JFrame();

        //Creates the Producer and Consumer to interact with the brokers of kafka
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
        String address = InputReader.obtainAnAddress();//Ask to the user the new address

        while (!done)
        {
            userProducer.sendAddressCustomer(nickname,address);//Send the address to the server
            eventsOfNotify = userConsumer.readEventsOfNotify(); //Reads the record with the response

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

        //Show result operation
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
