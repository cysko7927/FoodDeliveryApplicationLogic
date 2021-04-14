package org.deliveryfoodapp.ViewForUser;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.protocol.types.Field;
import org.deliveryfoodapp.broker.TopicManager;

import javax.swing.*;
import java.util.*;
import java.util.concurrent.ExecutionException;

public class CreateOrderView
{
    static private TopicManager topicManager;
    static private UserProducerShow userProducer;
    static private UserConsumer userConsumer;
    static public void executeOrderViewCreation(String nickname)
    {
        createsManagers(nickname);

        boolean done = false;
        JFrame frame = new JFrame();

        ConsumerRecords<String, String> eventsOfNotify = null;

        while (!done)
        {
            userProducer.askItem(nickname,"");//Ask the orders from the Server
            eventsOfNotify = userConsumer.readEventsOfNotify(); //Reads the record with all orders

            if (!eventsOfNotify.isEmpty())
                done = true;

        }

        String[][] allItems = new String[1][1];

        for (ConsumerRecord<String, String> record: eventsOfNotify)
        {
            allItems = obtainAllItemsAndQuantity(record.value());
        }

        done = false;
        String allitemString = "";
        int quantityItem;

        for (int i = 0;i< allItems.length;i++)
            allitemString = allitemString +allItems[i][0] + "," + allItems[i][1] + "\n";

        Map<String,Integer> itemsForOrder = new HashMap<>();

        while (!done)
        {
            int chosen = InputReader.readIntBeetween(0,allItems.length,allitemString);

            if (chosen == 0 && itemsForOrder.equals(""))
            {
                JOptionPane.showMessageDialog(frame,
                        "no Item selected impossible to terminate");
            }
            else if(chosen== 0)
            {
                done = true;
            }
            else
            {
                String item = allItems[chosen-1][0];
                quantityItem = InputReader.readIntGreaterOrEqualOf1();

                if (itemsForOrder.containsKey(item))//If the item is already inside the order
                {
                    itemsForOrder.replace(item,itemsForOrder.get(item) + quantityItem); //Update the quantity of the item inside the order
                    allItems[chosen-1][1] = String.valueOf(Integer.parseInt(allItems[chosen-1][1]) - quantityItem);//Update the quantity

                }
                else
                {
                    itemsForOrder.put(item,quantityItem);
                    allItems[chosen-1][1] = String.valueOf(Integer.parseInt(allItems[chosen-1][1]) - quantityItem);
                }

                allitemString = "";
                for (int i = 0;i< allItems.length;i++)
                    allitemString = allitemString +allItems[i][0] + "," + allItems[i][1] + "\n";
            }
        }

        String itemsForOrderString = "";

        for (String item:new ArrayList<>(itemsForOrder.keySet()))
        {
            itemsForOrderString = itemsForOrderString + item + "," + itemsForOrder.get(item) +"\n";
        }
        //Send the orders to the Producer
        userProducer.sendOrder(nickname,itemsForOrderString);
        eventsOfNotify = userConsumer.readEventsOfNotify(); //Wait the Notify from the Server

        for (ConsumerRecord<String, String> record: eventsOfNotify) //Print the message from the server
        {
            System.out.println(record.key());
            System.out.println(record.value());
            JOptionPane.showMessageDialog(frame,
                    record.key()+"\n" + record.value());
        }


        destroyManagers(nickname);

    }

    static public void executeOrdersView(String nickname)
    {

        createsManagers(nickname);


        boolean done = false;
        ConsumerRecords<String, String> eventsOfNotify = null;

        while (!done)
        {
            userProducer.askOrder(nickname,"");//Ask the orders from the Server
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

    private static String[][] obtainAllItemsAndQuantity(String allItems)
    {
        String[] allItemsVector = allItems.split(System.getProperty("line.separator"));

        String[][] matrix = new String[allItemsVector.length][2];

        for (int i = 0; i< allItemsVector.length;i++)
        {
            String[] itemAndQuantity = allItemsVector[i].split(",");
            matrix[i][0] = itemAndQuantity[0];
            matrix[i][1] = itemAndQuantity[1];

        }

        return matrix;
    }
}

