package br.com.piala.demo;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Properties;

import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;


public class HelloKafkaProducer {
    final static String TOPIC = "myfirsteventhub";

    public static void main(String[] argv){
        Properties properties = new Properties();
        properties.put("metadata.broker.list","129.144.160.38:6667");
        properties.put("serializer.class","kafka.serializer.StringEncoder");
        ProducerConfig producerConfig = new ProducerConfig(properties);
        kafka.javaapi.producer.Producer<String,String> producer = new kafka.javaapi.producer.Producer<String, String>(producerConfig);
        SimpleDateFormat sdf = new SimpleDateFormat();
        KeyedMessage<String, String> message =new KeyedMessage<String, String>(TOPIC,"Uma mensagem de demonstracao " + sdf.format(new Date()));
        producer.send(message);
        producer.close();
    }
}
