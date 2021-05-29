package org.deliveryfoodapp.Microservices.shippingServices;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.protocol.types.Field;
import org.deliveryfoodapp.Microservices.ProducerEventsForUser;
import org.deliveryfoodapp.broker.NameOfTopics;

public class ShippingServices
{
    ProducerEventsForUser producerEventsForShipping;
    ConsumerEventsForShipping consumerEventsForShipping;
    DbShipping dbShipping;

    public ShippingServices() {
        this.producerEventsForShipping = new ProducerEventsForUser();
        this.consumerEventsForShipping = new ConsumerEventsForShipping();
        this.dbShipping = new DbShipping();
    }

    public void executeServices()
    {
        ConsumerRecords<String, String> events = consumerEventsForShipping.readEventsOfShipment();
        for (final ConsumerRecord<String, String> record : events) //If there are events read from the broker
        {
            switch (record.topic())
            {
                case NameOfTopics
                        .shippingCreation:
                    //The record is like: key: nickname, value:KeyOrder
                    String keyOrder = record.value();
                    String nickname = record.key();

                    dbShipping.addShipment(nickname,keyOrder);


                    consumerEventsForShipping.commitState();
                    break;
                case NameOfTopics
                        .showShippingNotCompleted:
                    // record = key:nickname, value:

                    String allShipment = dbShipping.obtainAllShipmentNotCompleted();

                    if (allShipment == "Error")
                    {
                        producerEventsForShipping.sendRecordForATopic(NameOfTopics.notifyUser+record.key(),"Error in the Shipping services","impossible obtain all the shipment");
                        break;
                    }

                    consumerEventsForShipping.commitState();
                    producerEventsForShipping.sendRecordForATopic(NameOfTopics.notifyUser+record.key(),"Here there are all the not completed shipment",allShipment);
                    break;

                case NameOfTopics
                        .completeShipping:
                    //record = key:nickShippinMen,value:keyOrder,nickCustomer
                    String nickShippingMen = record.key();
                    String[] keyAndNick = record.value().split(",");

                    int status = dbShipping.completeAShipment(keyAndNick[0],keyAndNick[1]);

                    if (status == 2)
                        producerEventsForShipping.sendRecordForATopic(NameOfTopics.notifyUser+nickShippingMen,"Error in the Shipping services","error reading the DB");
                    else if(status == 1)
                        producerEventsForShipping.sendRecordForATopic(NameOfTopics.notifyUser+nickShippingMen,"Shipment yet Completed","Someone has notified the shipment meanwhile");
                    else
                        producerEventsForShipping.sendRecordForATopic(NameOfTopics.notifyUser+nickShippingMen,"Shipment notified and completed","the shipment "+keyAndNick[0] +" for user "+keyAndNick[1]+" has been notified");

                    consumerEventsForShipping.commitState();
                    break;

            }
        }
    }

    public static void main(String[] args) {

        ShippingServices services = new ShippingServices();

        while (true)
        {
            services.executeServices();
        }

    }
}
