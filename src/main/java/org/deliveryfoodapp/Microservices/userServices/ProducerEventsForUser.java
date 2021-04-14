package org.deliveryfoodapp.Microservices.userServices;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

public class ProducerEventsForUser {

    private static final String defaultTopic = "topicA";

    private static final int numMessages = 100000;
    private static final int waitBetweenMsgs = 500;
    private static final boolean waitAck = true;

    private static final String serverAddr = "localhost:9092";

    private final KafkaProducer<String, String> producer;
    private final Properties props;

    public ProducerEventsForUser() {
        props = new Properties(); //Creo le proprietà che deve avere il producer
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, serverAddr); //Indirizzo del server dove si trova il middleware
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName()); //Setto la chiave
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName()); //Setto il valore

        producer = new KafkaProducer<>(props); //Definite le proprietà le passo al costruttore

    }

    public void sendRecordForATopic(String topic,String key,String value)
    {
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
    }

    public void closeProducer()
    {
        producer.close();
    }
}
