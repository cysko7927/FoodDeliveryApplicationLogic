package org.deliveryfoodapp.ViewForUser;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.deliveryfoodapp.broker.TopicManager;

import javax.swing.*;
import java.util.concurrent.ExecutionException;

public class HandleItemsView
{
    static private TopicManager topicManager;
    static private UserProducerShow userProducer;
    static private UserConsumer userConsumer;

    public static void showItemView(String nickname)
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

        String nick= "";
        String message= "";
        boolean done = false;
        ConsumerRecords<String, String> eventsOfNotify = null;

        while (!done)
        {
            userProducer.askItem(nickname,"");//Ask the items from the Server
            eventsOfNotify = userConsumer.readEventsOfNotify(); //Reads the record with all orders

            if (!eventsOfNotify.isEmpty())
            {
                for (ConsumerRecord<String, String> record: eventsOfNotify) //Check the arrived records
                {
                    if (record.key().equals(nickname))
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
                "Message:"+message);


    }
    public static void addItemView(String nickname)
    {
        JFrame frame = new JFrame();

        String item = "";
        int quantity;
        //Ask the item (new/existing)
        String s = "";
        boolean done = false;

        while (!done)
        {
            //Ask the name of the item to the user
            s = (String)JOptionPane.showInputDialog(
                    frame,
                    "Insert the name of the item (to add/to update)",
                    "Insert Item",
                    JOptionPane.PLAIN_MESSAGE,
                    null,
                    null,
                    "");

            //If a string was returned, save it.
            if ((s != null) && (s.length() > 0)) {
                item = s;
                done = true;
            }
        }

        done = false;
        //Ask the quantity
        while (!done)
        {
            s = (String)JOptionPane.showInputDialog(
                    frame,
                    "Insert the new quantity of the item",
                    "Insert quantity",
                    JOptionPane.PLAIN_MESSAGE,
                    null,
                    null,
                    "");
            if (((s != null) && s.matches("-?\\d+(\\.\\d+)?") && Integer.parseInt(s) > 0)) //If the input is a number
                done = true;//Exit to the cycle
        }

        quantity = Integer.parseInt(s); //Save quantity

        try {
            createsManagers(nickname);
        } catch (Exception e) {
            destroyManagers(nickname);
            JOptionPane.showMessageDialog(frame,
                    "Error of connection:Retry o check the connection");
            return;
        }

        userProducer.sendItemNewOrUpdated(nickname,item+","+quantity); //Send the item and the quantity to the Server

        String nick= "";
        String message= "";
        done = false;
        ConsumerRecords<String, String> eventsOfNotify = null;

        eventsOfNotify = userConsumer.readEventsOfNotify(); //Wait the items of the Warehouse updated

        if (!eventsOfNotify.isEmpty())//If the response is not arrived
        {
            for (ConsumerRecord<String, String> record: eventsOfNotify) //Check the arrived records
            {
                if (record.key().equals(nickname))
                {
                    nick = record.key();
                    message = record.value();
                    done = true;
                }
            }
        }

        while (!done)//Continue to ask at the server the items in the Warehouse
        {
            userProducer.askItem(nickname,"");//Ask the items from the Server
            eventsOfNotify = userConsumer.readEventsOfNotify(); //Reads the record with all the items

            if (!eventsOfNotify.isEmpty())
            {
                for (ConsumerRecord<String, String> record: eventsOfNotify) //Check the arrived records
                {
                    if (record.key().equals(nickname))
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
                    "Message:"+"\n" + message);

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
