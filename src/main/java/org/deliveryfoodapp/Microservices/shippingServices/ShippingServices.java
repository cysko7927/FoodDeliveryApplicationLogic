package org.deliveryfoodapp.Microservices.shippingServices;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.protocol.types.Field;
import org.deliveryfoodapp.Microservices.ProducerEventsForUser;
import org.deliveryfoodapp.Microservices.orderServices.ConsumerEventsForOrder;
import org.deliveryfoodapp.broker.NameOfTopics;
import org.deliveryfoodapp.broker.NetworkBroker;
import org.deliveryfoodapp.broker.TopicManager;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

public class ShippingServices
{
    ProducerEventsForUser producerEventsForShipping;
    ConsumerEventsForShipping consumerEventsForShipping;
    DbShipping dbShipping;

    public ShippingServices()
    {
        this.dbShipping = new DbShipping();
    }

    public void createProducerAndConsumer()
    {
        if (this.consumerEventsForShipping !=null)
            this.consumerEventsForShipping.closeConsumer();
        if (this.producerEventsForShipping != null)
            this.producerEventsForShipping.closeProducer();

        this.consumerEventsForShipping = new ConsumerEventsForShipping();
        this.producerEventsForShipping = new ProducerEventsForUser();
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
                    //The record is like: key: nickname, value:KeyOrder,Address
                    String keyOrderAndAddress[] = record.value().split(",");

                    String nickname = record.key();

                    dbShipping.addShipment(nickname,keyOrderAndAddress[0],keyOrderAndAddress[1]);


                    consumerEventsForShipping.commitState();
                    break;
                case NameOfTopics
                        .showShippingNotCompleted:
                    // record = key:nickname, value:

                    String allShipment = dbShipping.obtainAllShipmentNotCompleted();

                    if (allShipment == "Error")
                    {
                        producerEventsForShipping.sendRecordForATopic(NameOfTopics.notifyUser, record.key(), "impossible obtain all the shipment");
                        break;
                    }

                    consumerEventsForShipping.commitState();
                    producerEventsForShipping.sendRecordForATopic(NameOfTopics.notifyUser,record.key(),allShipment);
                    break;

                case NameOfTopics
                        .completeShipping:
                    //record = key:nickShippinMen,value:keyOrder,nickCustomer
                    String nickShippingMen = record.key();
                    String[] keyAndNick = record.value().split(",");

                    int status = dbShipping.completeAShipment(keyAndNick[0],keyAndNick[1]);

                    if (status == 2)
                        producerEventsForShipping.sendRecordForATopic(NameOfTopics.notifyUser,nickShippingMen,"error reading the DB");
                    else if(status == 1)
                        producerEventsForShipping.sendRecordForATopic(NameOfTopics.notifyUser,nickShippingMen,"Someone has notified the shipment meanwhile");
                    else {
                        producerEventsForShipping.sendRecordForATopic(NameOfTopics.notifyCompletedShipping,keyAndNick[0],keyAndNick[1]);//Notify the orderService that a shipping is complete
                        producerEventsForShipping.sendRecordForATopic(NameOfTopics.notifyUser, nickShippingMen, "the shipment " + keyAndNick[0] + " for user " + keyAndNick[1] + " has been notified");
                    }
                    consumerEventsForShipping.commitState();
                    break;

            }
        }
    }

    public static void main(String[] args) throws IOException {

        List<String> servers = Files.lines(Paths.get("./config.txt")).collect(Collectors.toList());
        NetworkBroker.server0 = servers.get(0).split("=")[1];
        NetworkBroker.server1 = servers.get(1).split("=")[1];
        NetworkBroker.server2 = servers.get(2).split("=")[1];

        ShippingServices service = null;

        service = new ShippingServices();

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
