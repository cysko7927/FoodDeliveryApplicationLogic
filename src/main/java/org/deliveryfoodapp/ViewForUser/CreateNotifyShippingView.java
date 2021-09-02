package org.deliveryfoodapp.ViewForUser;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.deliveryfoodapp.Microservices.orderServices.DBorder;
import org.deliveryfoodapp.Microservices.shippingServices.DbShipping;
import org.deliveryfoodapp.broker.TopicManager;

import javax.swing.*;
import java.util.concurrent.ExecutionException;

public class CreateNotifyShippingView
{
    static private UserProducerShow userProducer;
    static private UserConsumer userConsumer;


    public static void showShippingNotCompleteView(String nickname)
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

        String nick= "";
        String message= "";
        boolean done = false;
        ConsumerRecords<String, String> eventsOfNotify = null;


        while (!done)
        {
            userProducer.askShippingNotCompleted(nickname,"");//Ask the shipping to notify from the Server
            eventsOfNotify = userConsumer.readEventsOfNotify(); //Reads the record with all the shipping

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

        //Close the Producer and Consumer for now
        destroyManagers(nickname);

        String[] selectedShipping = new String[2];

            if (!message.equals(DbShipping.noShippingToNotify))//If there are shipping to notify
            {
                String[] allShippingNotCompleted  = message.split(System.getProperty("line.separator"));
                int selected = GuiForNotifyView.askIndexShippingToNotify(1,allShippingNotCompleted.length,message);//Print the Shipping not notified from the server
                selectedShipping = allShippingNotCompleted[selected-1].split(","); //And ask at the user to select a shipping

                //Creates the Producer and Consumer to interact with the brokers of kafka
                try {
                    createsManagers(nickname);
                } catch (Exception e) {
                    destroyManagers(nickname);
                    JOptionPane.showMessageDialog(frame,
                            "Error of connection:Retry o check the connection");
                    return;
                }

                userProducer.sendNotificationForShipping(selectedShipping[0],selectedShipping[1],nickname); //send notification to the broker
                ConsumerRecords<String, String> eventsOfNotify1  = userConsumer.readEventsOfNotify();

                for (ConsumerRecord<String, String> record: eventsOfNotify1) //Print the message from the server
                {
                    if (record.key().equals(nickname))
                    {
                        nick = record.key();
                        message = record.value();
                    }

                }
                //Closes the Producer and Consumer to interact with the brokers of kafka
                destroyManagers(nickname);

                JOptionPane.showMessageDialog(frame,
                        "Message:"+message);
            }
            else //If there aren't shipping to notify
            {
                JOptionPane.showMessageDialog(frame,
                        "There aren't shipping to notify");// Says to the user that there aren't shipping
            }


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

class GuiForNotifyView
{
    public static int askIndexShippingToNotify(int min,int max,String text)
    {

        JFrame frame = new JFrame();
        String str= (String) JOptionPane.showInputDialog(
                frame,
                text,
                "Insert the index of the Shipping to notify",
                JOptionPane.PLAIN_MESSAGE,
                null,
                null,
                "");

        while(!(str != null && str.matches("-?\\d+(\\.\\d+)?") && Integer.parseInt(str) >= min && Integer.parseInt(str) <= max))
        {
            JOptionPane.showMessageDialog(frame,
                    "index is not valid");
            str = (String) JOptionPane.showInputDialog(
                    frame,
                    text,
                    "Insert the index of the Shipping to notify",
                    JOptionPane.PLAIN_MESSAGE,
                    null,
                    null,
                    "");
        }

        return Integer.parseInt(str);

    }

}
