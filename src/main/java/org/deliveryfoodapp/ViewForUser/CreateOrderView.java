package org.deliveryfoodapp.ViewForUser;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.protocol.types.Field;
import org.deliveryfoodapp.broker.NetworkBroker;
import org.deliveryfoodapp.broker.TopicManager;

import javax.swing.*;
import java.util.*;
import java.util.concurrent.ExecutionException;

/**
 * todo: put the check address shipping in the order Creation
 */

public class CreateOrderView
{
    static private UserProducerShow userProducer;
    static private UserConsumer userConsumer;
    static public void executeOrderViewCreation(String nickname)
    {
        JFrame frame = new JFrame();

        try {
            createsManagers(nickname);
        } catch (ExecutionException e) {
            destroyManagers(nickname);
            JOptionPane.showMessageDialog(frame,
                    "Error of connection:Retry o check the connection");
            return;
        } catch (InterruptedException e) {
            destroyManagers(nickname);
            JOptionPane.showMessageDialog(frame,
                    "Error of connection:Retry o check the connection");
            return;
        }

        String address = obtainAddressShipping(nickname); //Check if the user has an address for shipping

        if (address == "") //If the user has not an address for Shipping
        {
            destroyManagers(nickname);
            JOptionPane.showMessageDialog(frame,
                    "Dear user:"+ nickname +"You didn't set an address, impossible to do an order");
            return; //Stop creation order

        }



        boolean done = false;


        ConsumerRecords<String, String> eventsOfNotify = null;
        String nick = "";
        String message = "";

        while (!done)
        {
            userProducer.askItem(nickname,"");//Ask the orders from the Server
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

        destroyManagers(nickname);//Free the resource for now

        if (message.equals("A quantity of a product is not valid"))
        {
            JOptionPane.showMessageDialog(frame,
                    "Message:"+"\n"+message);
            return;
        }
        String[][] allItems;

        allItems = obtainAllItemsAndQuantity(message);


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

        try
        {
            createsManagers(nickname); //Starts again the manager
        }
        catch (Exception e) {
            destroyManagers(nickname);
            JOptionPane.showMessageDialog(frame,
                    "Error of connection:Retry o check the connection");
            return;
        }

        //Send the orders to the Producer
        userProducer.sendOrder(nickname,address + "\n" + itemsForOrderString);
        eventsOfNotify = userConsumer.readEventsOfNotify(); //Wait the Notify from the Server

        for (ConsumerRecord<String, String> record: eventsOfNotify) //Print the message from the server
        {
            if (record.key().equals(nickname))
            {
                nick = record.key();
                message = record.value();
            }

        }

        destroyManagers(nickname);
        JOptionPane.showMessageDialog(frame,
                "Message:"+"\n"+message);

    }

    static public void executeOrdersView(String nickname)
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
            userProducer.askOrder(nickname,"");//Ask the orders from the Server
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
                "Here there are your orders:" +"\n" + message);

    }

    //Ask to the server the data of a user and retrieves the Address if exists
    private static String obtainAddressShipping(String nickname)
    {
        String nick= "";
        String message= "";
        boolean done = false;
        ConsumerRecords<String, String> eventsOfNotify = null;

        while (!done)
        {
            userProducer.askUserData(nickname,"");//Ask the data User from the Server
            eventsOfNotify = userConsumer.readEventsOfNotify(); //Reads the records

            if (!eventsOfNotify.isEmpty())
            {
                for (ConsumerRecord<String, String> record: eventsOfNotify) //Check the arrived records
                {
                    if (record.key().equals(nickname)) //Take only the record for the user
                    {
                        nick = record.key();
                        message = record.value();
                        done = true;
                    }

                }
            }
        }

        String[] dataUser = message.split(",");//obtain the data

        if (dataUser.length == 4) //If there is the address
            return dataUser[3];//returns it

        return "";
    }

    private static void createsManagers(String nickname) throws ExecutionException, InterruptedException//Create producer, consumers and Topic Manager
    {
        userProducer = new UserProducerShow();
        userConsumer = new UserConsumer(nickname);

    }

    private static void destroyManagers(String nickname)//Destroy producer, consumers and Topic Manager
    {
        userProducer.closeProducer();
        userConsumer.closeConsumer();
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

