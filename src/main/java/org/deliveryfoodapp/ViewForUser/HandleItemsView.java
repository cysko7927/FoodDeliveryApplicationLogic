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
    public static void addItemView(String nickname)
    {
        createsManagers(nickname);

        String item = "";
        int quantity;
        //Ask the item (new/existing)
        JFrame frame = new JFrame();

        boolean done = false;

        //Ask the name of the item to the user
        String s = (String)JOptionPane.showInputDialog(
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
        }

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

        userProducer.sendItemNewOrUpdated(nickname,item+","+quantity); //Send the item and the quantity to the Server

        done = false;
        ConsumerRecords<String, String> eventsOfNotify = null;

        eventsOfNotify = userConsumer.readEventsOfNotify(); //Wait the items of the Warehouse updated

        if (!eventsOfNotify.isEmpty())//If the response is not arrived
            done = true;

        while (!done)//Continue to ask at the server the items in the Warehouse
        {
            userProducer.askItem(nickname,"");//Ask the items from the Server
            eventsOfNotify = userConsumer.readEventsOfNotify(); //Reads the record with all the items

            if (!eventsOfNotify.isEmpty())
                done = true;

        }

        frame = new JFrame();

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
