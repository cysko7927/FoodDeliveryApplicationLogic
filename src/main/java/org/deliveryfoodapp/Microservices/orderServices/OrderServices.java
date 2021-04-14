package org.deliveryfoodapp.Microservices.orderServices;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.deliveryfoodapp.Microservices.userServices.ConsumerEventsForUser;
import org.deliveryfoodapp.Microservices.userServices.DBuser;
import org.deliveryfoodapp.Microservices.userServices.ProducerEventsForUser;
import org.deliveryfoodapp.Model.Order;
import org.deliveryfoodapp.broker.NameOfTopics;

import java.io.IOException;

public class OrderServices {

    ProducerEventsForOrder producerEventsForOrder;
    ConsumerEventsForOrder consumerEventsForOrder;
    DBitem dbItem;
    DBorder dbOrder;

    public OrderServices()
    {
        this.consumerEventsForOrder = new ConsumerEventsForOrder();
        this.producerEventsForOrder = new ProducerEventsForOrder();
        this.dbItem = new DBitem();
        this.dbOrder = new DBorder();
    }

    public void executeServices() throws IOException, InterruptedException {
        ConsumerRecords<String, String> events = consumerEventsForOrder.readEventsOfRegistration();
        for (final ConsumerRecord<String, String> record : events) //If there are events read from the broker
        {
            // Write in the DB the credentials of the user
            switch (record.topic())
            {
                case NameOfTopics
                        .orderCreation:
                    //The record is like: key: nickname value:item1,quantity\nitem2,quantity\n.....
                    //Controllare se gli item sono disponibili
                    String nickuser = record.key();
                    boolean orderIsValid = true;

                    //The value of the record is a string like this: item1,quantity\nitem2,quantity\n.....
                    String[] lines = record.value().split(System.getProperty("line.separator"));//Read the values of the record

                    for (int i = 0; i< lines.length;i++)
                    {
                        String[] itemAndQuantity = lines[i].split(",");
                        int quantity = dbItem.obtainQuantity(itemAndQuantity[0]);

                        if (!(quantity > Integer.parseInt(itemAndQuantity[1]))) //If there aren't enough item for the purchase
                        {
                            orderIsValid = false;//Change the state of the order
                        }
                    }

                    if (!orderIsValid)//If the order is not valid
                    {
                        //Mandare messaggio di errore all'utente
                        producerEventsForOrder.sendRecordForATopic(NameOfTopics.notifyUser+nickuser,"Order is not valid","A quantity of a product is not valid");

                        break;//Interrupt the proccessing for this record
                    }

                    //Validifica e salva ordine nel DB

                    for (int i = 0; i< lines.length;i++) //Reduce the quantity of the items from the magazine
                    {
                        String[] itemAndQuantity = lines[i].split(",");
                        int quantity = dbItem.obtainQuantity(itemAndQuantity[0]); //Obtain quantity in the Warehouse

                        dbItem.modifyQuantity(itemAndQuantity[0],quantity - Integer.parseInt(itemAndQuantity[1]));
                    }

                    int keyOrder = dbOrder.addOrder(record.value().replace("\n",","), nickuser); //Write the order in the DB

                    if (keyOrder == 0)
                    {
                        producerEventsForOrder.sendRecordForATopic(NameOfTopics.notifyUser+nickuser,"Error in the orderServices","Error in accessing the DB");
                        break;
                    } //If there were errors in the DB to write the order then generates exception and send a message to the user



                    consumerEventsForOrder.commitState();//Commit to the Broker the state
                    //Invia evento creazione di spedizione annessa allo shippingServices
                    producerEventsForOrder.sendRecordForATopic(NameOfTopics.notifyUser+nickuser,"OrderCreated","Your order has been registered");
                    //producerEventsForOrder.sendRecordForATopic(NameOfTopics.shippingCreation,nickuser,String.valueOf(keyOrder));

                    break;
                case NameOfTopics.updateQuantityItem://If a Admin asked to update the quantity of a item
                    //the record is like this: key:nickname value:item,quantity

                    //Aggiorna quantità item nel DB degli item
                    String[] itemAndQuantity = record.value().split(",");//Split item and quantity

                    dbItem.modifyQuantity(itemAndQuantity[0], Integer.parseInt(itemAndQuantity[1]));

                    String allItem = dbItem.obtainAllitems();
                    if (allItem == "Error") //If there was an error in obtain all items
                    {
                        //Send a message of Error
                        producerEventsForOrder.sendRecordForATopic(NameOfTopics.notifyUser+record.key(),"Error in the orderServices","impossible modify quantity");
                        break;
                    }

                    //Crea evento notifyUser con gli item aggiornati
                    consumerEventsForOrder.commitState();//Commit to the Broker the state
                    producerEventsForOrder.sendRecordForATopic(NameOfTopics.notifyUser +record.key(),"item quantity added",allItem);
                    break;
                case NameOfTopics.showOrder: //if a customer asked to see his order
                    //The record is like: key: nickname value:

                    //Crea Evento notifyUser con tutti gli ordini dello user che ha fatto richiesta nel vedere i suoi ordini

                    String allOrders = dbOrder.obtainAllOrdersOfAUser(record.key());

                    if (allOrders == "Error") {
                        producerEventsForOrder.sendRecordForATopic(NameOfTopics.notifyUser + record.key(), "Error in the orderServices", "impossible obtain all the orders");
                        break;
                    }

                    consumerEventsForOrder.commitState();//Commit to the Broker the state
                    producerEventsForOrder.sendRecordForATopic(NameOfTopics.notifyUser +record.key(),"Here are your orders",allOrders);
                    break;
                case NameOfTopics.showItem:
                    //Crea Evento notifyUser con tutti gli item da inviare all'admin che ne ha fatto richiesta
                    //The record is like: key: nickname value:
                    String allItem1 = dbItem.obtainAllitems();
                    if (allItem1 == "Error") //If there was an error in obtain all items
                    {
                        //Send a message of Error
                        producerEventsForOrder.sendRecordForATopic(NameOfTopics.notifyUser+record.key(),"Error in the orderServices","impossible modify quantity");
                        break;
                    }

                    //Else
                    consumerEventsForOrder.commitState();//Commit to the Broker the state
                    producerEventsForOrder.sendRecordForATopic(NameOfTopics.notifyUser +record.key(),"Here there are all the items",allItem1);
                    break;
            }



        }
    }

    public static void main(String[] args) throws IOException, InterruptedException {
        OrderServices service = new OrderServices();

        while (true){

            service.executeServices();


        }
    }
}
