package org.deliveryfoodapp.Microservices.userServices;

import org.apache.kafka.clients.admin.*;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.deliveryfoodapp.Microservices.ProducerEventsForUser;
import org.deliveryfoodapp.Microservices.orderServices.ConsumerEventsForOrder;
import org.deliveryfoodapp.Model.User;
import org.deliveryfoodapp.broker.NameOfTopics;
import org.deliveryfoodapp.broker.NetworkBroker;
import org.deliveryfoodapp.broker.TopicManager;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

public class UserServices
{
    ProducerEventsForUser producerEventsForUser;
    ConsumerEventsForUser consumerEventsForUser;
    DBuser dbUser;

    public void createProducerAndConsumer()
    {
        if (this.consumerEventsForUser !=null)
            this.consumerEventsForUser.closeConsumer();
        if (this.producerEventsForUser != null)
            this.producerEventsForUser.closeProducer();

        this.consumerEventsForUser = new ConsumerEventsForUser();
        this.producerEventsForUser = new ProducerEventsForUser();
    }

    public UserServices()
    {
        this.dbUser = new DBuser();
    }

    public void executeServices()
    {
        ConsumerRecords<String, String> events = consumerEventsForUser.readEventsOfRegistration();
        for (final ConsumerRecord<String, String> record : events) //If there are events readed from the broker
        {

            switch (record.topic())
            {
                case NameOfTopics
                        .userRegistration:
                    // Write in the DB the credentials of the user

                    String[] lines = record.value().split(System.getProperty("line.separator"));//Read the values of the record
                    dbUser.writeCredentialsUser(lines[0],lines[1],lines[2]);//Write in the DB the credentials


                    break;
                case NameOfTopics.showUserData:
                    //The record is like: key: nickname value:

                    String nickuser = record.key();

                    List<String> result = dbUser.searchUser(nickuser);

                    //Send the response message
                    producerEventsForUser.sendRecordForATopic(NameOfTopics.notifyUser, record.key(), result.get(0));
                    break;
                case NameOfTopics.updateAddressShippingUser:
                    //The record is like: key: nickname value: address

                    int outcome = dbUser.updateAddressShipping(record.key(),record.value());

                    if (outcome == 1)
                    {
                        producerEventsForUser.sendRecordForATopic(NameOfTopics.notifyUser,record.key(),"Error in the server");
                    }
                    else
                    {
                        producerEventsForUser.sendRecordForATopic(NameOfTopics.notifyUser,record.key(),"Address added correctly");
                    }

                    break;
            }

        }

        consumerEventsForUser.commitState();//Commit to the Broker the state
    }

    public static void main(String[] args) throws IOException {
        List<String> servers = Files.lines(Paths.get("./config.txt")).collect(Collectors.toList());
        NetworkBroker.server0 = servers.get(0).split("=")[1];
        NetworkBroker.server1 = servers.get(1).split("=")[1];
        NetworkBroker.server2 = servers.get(2).split("=")[1];
        UserServices userServices = null;

        userServices = new UserServices();

        userServices.createProducerAndConsumer();

        while (true)
        {
            try {
                userServices.executeServices();
            } catch (Exception e) { //If there are errors about Broker
                userServices.createProducerAndConsumer();
            }

        }

    }

}


