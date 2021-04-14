package org.deliveryfoodapp.ViewForUser;

import com.fasterxml.jackson.databind.ser.impl.StringArraySerializer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.protocol.ObjectSerializationCache;
import org.apache.kafka.common.serialization.StringSerializer;
import org.deliveryfoodapp.Model.TypeOfUser;
import org.deliveryfoodapp.broker.NameOfTopics;

import java.util.Properties;
import java.util.Random;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

public class UserProducer
{
    private static final boolean waitAck = true;
    private static final int waitBetweenMsgs = 500;

    private static final String serverAddr = "localhost:9092";


    public static void sendRegistrationAtBroker(String nick, String password, TypeOfUser type)
    {
        final Properties props = new Properties(); //Creo le proprietà che deve avere il producer
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, serverAddr); //Indirizzo del server dove si trova il middleware
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName()); //Setto la chiave
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName()); //Setto il valore

        final KafkaProducer<String, String> producer = new KafkaProducer<>(props); //Definite le proprietà le passo al costruttore
        final Random r = new Random();


            final String topic = NameOfTopics.userRegistration; //Topic
            final String key = "Registration:" + nick;

            //**************************************************************************
            final String value = nick+ "\n" + password + "\n" + convertTypeInString(type);
            //**************************************************************************


            System.out.println(
                    "Topic: " + topic +
                            "\tKey: " + key +
                            "\tValue: " + value
            );

            final ProducerRecord<String, String> record = new ProducerRecord<>(topic, key, value); //Creo il record che deve inviare il producer
            final Future<RecordMetadata> future = producer.send(record);//Dico al producer di inviare il record e ritorna il future

            if (waitAck) {
                try {
                    RecordMetadata ack = future.get(); //Aspetto l'ack
                    System.out.println("Ack for topic " + ack.topic() + ", partition " + ack.partition() + ", offset " + ack.offset());
                } catch (InterruptedException | ExecutionException e1) {
                    e1.printStackTrace();
                }
            }

            try {
                Thread.sleep(waitBetweenMsgs);
            } catch (final InterruptedException e) {
                e.printStackTrace();
            }


        producer.close(); //Spengo producer
    }


    /**
     * Precondition: the type must be correct
     * @param type type to convert in a String
     * @return String that represents  the type of user
     */
    private static String convertTypeInString(TypeOfUser type)
    {
        switch (type)
        {
            case ADMIN:
                return "ADMIN";
            case CUSTOMER:
                return "CUSTOMER";
            case SHIPPINGMEN:
                return "SHIPPINGMEN";
            default:
                return "";
        }
    }
}
