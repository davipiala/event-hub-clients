package br.com.piala.demo;

import java.util.Arrays;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;


public class HelloKafkaConsumer extends  Thread {


    public static void main(String[] args) {
        String topicName = "myfirsteventhub";
        String bootstrapServer = "129.144.160.38:6667";
        String group = "test_group";

        System.out.println("Ouvindo topico: " + topicName);
        Properties props = new Properties();
        props.put("bootstrap.servers", bootstrapServer);
        props.put("group.id", group);
        props.put("enable.auto.commit", "true");
        props.put("auto.commit.interval.ms", "1000");
        props.put("session.timeout.ms", "30000");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        KafkaConsumer consumer = new KafkaConsumer(props);
        consumer.subscribe(Arrays.asList(topicName));
        System.out.println("Inscrito no tópico: " + consumer.subscription());

        while (true) {
            ConsumerRecords<byte[], byte[]> records = consumer.poll(1000);
            for (ConsumerRecord<byte[], byte[]> record : records) {
                System.out.printf("Mensagem do tópico =%s, partition =%s, offset = %d,  valor = %s\n", record.topic(), record.partition(), record.offset(), record.value());
            }

        }
    }
    
    private static <V> V deserialize(final byte[] objectData) {
        return (V) org.apache.commons.lang3.SerializationUtils.deserialize(objectData);
    }
    
    
}
