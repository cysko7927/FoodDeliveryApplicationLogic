package org.deliveryfoodapp.Microservices.orderServices;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.deliveryfoodapp.Microservices.ProducerEventsForUser;
import org.deliveryfoodapp.broker.NameOfTopics;
import org.deliveryfoodapp.broker.NetworkBroker;
import org.deliveryfoodapp.broker.TopicManager;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;


public class OrderServices {

    ProducerEventsForUser producerEventsForOrder = null;
    ConsumerEventsForOrder consumerEventsForOrder = null;
    DBitem dbItem;
    DBorder dbOrder;

    public OrderServices() {
        this.dbItem = new DBitem();
        this.dbOrder = new DBorder();

    }

    public void createProducerAndConsumer()
    {
        if (this.consumerEventsForOrder !=null)
            this.consumerEventsForOrder.closeConsumer();
        if (this.producerEventsForOrder != null)
            this.producerEventsForOrder.closeProducer();

        this.consumerEventsForOrder = new ConsumerEventsForOrder();
        this.producerEventsForOrder = new ProducerEventsForUser();
    }



    public void executeServices() throws IOException, InterruptedException {
        ConsumerRecords<String, String> events = consumerEventsForOrder.readEventsOfOrdersOrItem();
        for (final ConsumerRecord<String, String> record : events) //If there are events read from the broker
        {
            // Write in the DB the credentials of the user
            switch (record.topic())
            {
                case NameOfTopics
                        .orderCreation:
                    //The record is like: key: nickname value:address\nitem1,quantity\nitem2,quantity\n.....
                    //Check if the items are available
                    String nickuser = record.key();
                    boolean orderIsValid = true;

                    //The value of the record is a string like this: address\nitem1,quantity\nitem2,quantity\n.....
                    String[] lines = record.value().split(System.getProperty("line.separator"));//Read the values of the record

                    for (int i = 1; i< lines.length;i++)
                    {
                        String[] itemAndQuantity = lines[i].split(",");
                        int quantity = dbItem.obtainQuantity(itemAndQuantity[0]);

                        if (!(quantity >= Integer.parseInt(itemAndQuantity[1]))) //If there aren't enough item for the purchase
                        {
                            orderIsValid = false;//Change the state of the order
                        }
                    }

                    if (!orderIsValid)//If the order is not valid
                    {
                        // Send error message to user
                        producerEventsForOrder.sendRecordForATopic(NameOfTopics.notifyUser,nickuser,"A quantity of a product is not valid");

                        break;//Interrupt the proccessing for this record
                    }

                    // Validate and save the order in the DB

                    for (int i = 1; i< lines.length;i++) //Reduce the quantity of the items from the magazine
                    {
                        String[] itemAndQuantity = lines[i].split(",");
                        int quantity = dbItem.obtainQuantity(itemAndQuantity[0]); //Obtain quantity in the Warehouse

                        dbItem.modifyQuantity(itemAndQuantity[0],quantity - Integer.parseInt(itemAndQuantity[1]));//Modify quantity
                    }

                    int keyOrder = dbOrder.addOrder(record.value().replace("\n",","), nickuser); //Write the order in the DB

                    if (keyOrder == 0)
                    {
                        producerEventsForOrder.sendRecordForATopic(NameOfTopics.notifyUser,nickuser,"Error in accessing the DB");
                        break;
                    } //If there were errors in the DB to write the order then generates exception and send a message to the user


                    //Sending message to notify the correct Creation of the order
                    producerEventsForOrder.sendRecordForATopic(NameOfTopics.notifyUser,nickuser,"Your order has been registered");
                    //Sending a record for the shipping Address to create the shipping (contains the nick, keyorder and Address shipping
                    producerEventsForOrder.sendRecordForATopic(NameOfTopics.shippingCreation,nickuser,keyOrder+ ","+lines[0]);

                    break;
                case NameOfTopics.updateQuantityItem://If a Admin asked to update the quantity of a item
                    //the record is like this: key:nickname value:item,quantity

                    // Update item quantity in the item DB
                    String[] itemAndQuantity = record.value().split(",");//Split item and quantity

                    dbItem.modifyQuantity(itemAndQuantity[0], Integer.parseInt(itemAndQuantity[1]));

                    String allItem = dbItem.obtainAllitems();
                    if (allItem == "Error") //If there was an error in obtain all items
                    {
                        //Send a message of Error
                        producerEventsForOrder.sendRecordForATopic(NameOfTopics.notifyUser,record.key(),"impossible modify quantity");
                        break;
                    }

                    // Create notifyUser event with updated items
                    producerEventsForOrder.sendRecordForATopic(NameOfTopics.notifyUser,record.key(),allItem);
                    break;
                case NameOfTopics.showOrder: //if a customer asked to see his order
                    //The record is like: key: nickname value:

                    // Create Event notifyUser with all orders of the user who requested to see his orders

                    String allOrders = dbOrder.obtainAllOrdersOfAUser(record.key());

                    if (allOrders == "Error") {
                        producerEventsForOrder.sendRecordForATopic(NameOfTopics.notifyUser, record.key(), "impossible obtain all the orders");
                        break;
                    }

                    producerEventsForOrder.sendRecordForATopic(NameOfTopics.notifyUser,record.key(),allOrders);
                    break;
                case NameOfTopics.showItem:
                    //Crea Evento notifyUser con tutti gli item da inviare all'admin che ne ha fatto richiesta
                    //The record is like: key: nickname value:
                    String allItem1 = dbItem.obtainAllitems();
                    if (allItem1 == "Error") //If there was an error in obtain all items
                    {
                        //Send a message of Error
                        producerEventsForOrder.sendRecordForATopic(NameOfTopics.notifyUser, record.key(), "impossible modify quantity");
                        break;
                    }

                    //Else send the result of the operation
                    producerEventsForOrder.sendRecordForATopic(NameOfTopics.notifyUser, record.key(), allItem1);
                    break;

                case NameOfTopics.notifyCompletedShipping:
                    //Record: key=keyOrder,value = nickCustomer

                    //Change the status of the order in the DB
                    dbOrder.changeStateOrder(record.key(), record.value());

                    break;

            }



        }
        consumerEventsForOrder.commitState();//Commit to the Broker the state
    }

    public static void main(String[] args) throws IOException {

        List<String> servers = Files.lines(Paths.get("./config.txt")).collect(Collectors.toList());
        NetworkBroker.server0 = servers.get(0).split("=")[1];
        NetworkBroker.server1 = servers.get(1).split("=")[1];
        NetworkBroker.server2 = servers.get(2).split("=")[1];

        OrderServices service = null;


        service = new OrderServices();

        /*boolean connection_done = false;

        try {
            //service.createTopics();
            connection_done = true;
        } catch (ExecutionException e) {
            NetworkBroker.switchServer();
        } catch (InterruptedException e) {
            NetworkBroker.switchServer();
        }

        if (!connection_done)
        {
            try {
                //service.createTopics();
                connection_done = true;
            } catch (ExecutionException e) {
                System.out.println("Servers not Available");
                return;
            } catch (InterruptedException e) {
                System.out.println("Servers not Available");
                return;
            }
        }
        */
        service.createProducerAndConsumer();


        while (true)
        {
            try {
                service.executeServices();
            } catch (Exception e) { //If there are errors about Broker
                service.createProducerAndConsumer();
            }
        }
    }
}
