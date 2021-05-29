package org.deliveryfoodapp.Microservices.userServices;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.deliveryfoodapp.Microservices.ProducerEventsForUser;
import org.deliveryfoodapp.Model.User;
import org.deliveryfoodapp.broker.NameOfTopics;

import java.util.List;

public class UserServices
{
    ProducerEventsForUser producerEventsForUser;
    ConsumerEventsForUser consumerEventsForUser;
    DBuser dbUser;

    public UserServices(ConsumerEventsForUser consumerEventsForUser)
    {
        this.consumerEventsForUser = new ConsumerEventsForUser();
        this.producerEventsForUser = new ProducerEventsForUser();
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
                    //Send the response message
                    String nickuser = record.key();

                    List<String> result = dbUser.searchUser(nickuser);


                    producerEventsForUser.sendRecordForATopic(NameOfTopics.notifyUser+nickuser,"Your Credentials",result.get(0));
                    break;
            }

        }

        consumerEventsForUser.commitState();//Commit to the Broker the state
    }

    public static void main(String[] args)
    {
        UserServices userServices = new UserServices(new ConsumerEventsForUser());

        while (true)
        {
            userServices.executeServices();
        }

    }

}
