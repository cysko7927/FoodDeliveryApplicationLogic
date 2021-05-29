package org.deliveryfoodapp.ViewForUser;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.deliveryfoodapp.broker.TopicManager;

import javax.swing.*;
import java.util.concurrent.ExecutionException;

public class CreateNotifyShippingView
{
    static private TopicManager topicManager;
    static private UserProducerShow userProducer;
    static private UserConsumer userConsumer;


    public static void showShippingNotCompleteView(String nickname)
    {

        createsManagers(nickname);

        boolean done = false;
        ConsumerRecords<String, String> eventsOfNotify = null;
        JFrame frame = new JFrame();

        while (!done)
        {
            userProducer.askShippingNotCompleted(nickname,"");//Ask the items from the Server
            eventsOfNotify = userConsumer.readEventsOfNotify(); //Reads the record with all orders

            if (!eventsOfNotify.isEmpty())
                done = true;

        }
        String[] selectedShipping = new String[2];
        for (ConsumerRecord<String, String> record: eventsOfNotify) //Print the Shipping not notified from the server
        {
            String[] allShippingNotCompleted  = record.value().split(System.getProperty("line.separator"));
            int selected = GuiForNotifyView.askIndexShippingToNotify(1,allShippingNotCompleted.length,record.value());
            selectedShipping = allShippingNotCompleted[selected-1].split(",");
        }


        userProducer.sendNotificationForShipping(selectedShipping[0],selectedShipping[1],nickname);
        eventsOfNotify = userConsumer.readEventsOfNotify();

        for (ConsumerRecord<String, String> record: eventsOfNotify) //Print the message from the server
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
