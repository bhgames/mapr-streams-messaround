package com.mapr.examples;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.google.common.io.Resources;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;

import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;
import java.util.Properties;
import java.util.Random;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.util.Date;
import java.text.SimpleDateFormat;
import java.util.Iterator;

public class Consumer {
    public static void main(String[] args) throws IOException {
        ObjectMapper mapper = new ObjectMapper();

        int msgCounter = 0;
        int timeouts = 0;
        String currentTSP = new SimpleDateFormat("yyyyMMddhhmm").format(new Date());
        String logFile = "/mapr/mapr-us-prod.avant.com/user/mapr/logs/log_" + currentTSP + ".json";
        File file = new File(logFile);
        FileWriter fw = new FileWriter(logFile);
        BufferedWriter bw = new BufferedWriter(fw);

        final String PAYMENT_TRANSACTIONS = "/user/mapr/streams:payment_transactions";

        KafkaConsumer<String, String> consumer;
        try (InputStream props = Resources.getResource("consumer.props").openStream()) {
            Properties properties = new Properties();
            properties.load(props);
            if (properties.getProperty("group.id") == null) {
                properties.setProperty("group.id", "group-" + new Random().nextInt(100000));
            }

            consumer = new KafkaConsumer<>(properties);
        }
        consumer.subscribe(Arrays.asList(PAYMENT_TRANSACTIONS));
        boolean stop = false;
        while (!stop) {
            ConsumerRecords<String, String> records = consumer.poll(200);

            if (records.count() == 0) {
                timeouts++;
            } else {
                    System.out.printf("Got %d records after %d timeouts\n", records.count(), timeouts);
                timeouts = 0;
            }
            for (ConsumerRecord<String, String> record : records) {
                if(msgCounter == 1000) {
                    if (bw != null) {
                        bw.close();
                        fw.close();
                        msgCounter = 0;
                    }
                    currentTSP = new SimpleDateFormat("yyyyMMddhhmm").format(new Date());
                    logFile = "/mapr/mapr-us-prod.avant.com/user/mapr/logs/log_" + currentTSP + ".json";
                    file = new File(logFile);
                    fw = new FileWriter(file.getAbsoluteFile());
                    bw = new BufferedWriter(fw);
                    // if file doesnt exists, then create it
                    if (!file.exists()) {
                        file.createNewFile();
                     }
                }
                switch (record.topic()) {
                    case PAYMENT_TRANSACTIONS:
                        msgCounter++;
                        JsonNode msgNode = mapper.readTree(record.value());
                        System.out.println("msg # " + msgCounter);
                        bw.write(msgNode.toString() + "\n");
                        bw.flush();
                        break;
                    default:
                        throw new IllegalStateException("Shouldn't be possible to get message on topic " + record.topic());
                }
            }
        }
    }
}