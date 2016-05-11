package com.mapr.examples;

import com.google.common.io.Resources;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.io.BufferedReader;
import java.util.ArrayList;
import java.util.List;
import java.util.HashMap;
import java.io.FileReader;

import com.google.gson.Gson;
import java.security.Timestamp;

import org.supercsv.io.ICsvListReader;
import org.supercsv.io.CsvListReader;
import org.supercsv.prefs.CsvPreference;
import org.supercsv.cellprocessor.ift.CellProcessor;
import org.supercsv.cellprocessor.constraint.NotNull;
import org.supercsv.cellprocessor.constraint.UniqueHashCode;
import org.supercsv.cellprocessor.Optional;
import org.supercsv.cellprocessor.ParseDate;

import org.supercsv.cellprocessor.CellProcessorAdaptor;
import org.supercsv.cellprocessor.ift.CellProcessor;
import org.supercsv.exception.SuperCsvCellProcessorException;
import org.supercsv.util.CsvContext;
import org.yaml.snakeyaml.Yaml;
import java.util.Random;
/**
 * This producer will send a bunch of messages to topic "fast-messages". Every so often,
 * it will send a message to "slow-messages". This shows how messages can be sent to
 * multiple topics. On the receiving end, we will see both kinds of messages but will
 * also see how the two topics aren't really synchronized.
 */
public class CDCProducerSim {
    public static void main(String[] args) throws IOException {

        final String TOPIC = "/user/mapr/streams:payment_transactions";
        // TODO Bedrock ingest adding a " to the end of each file
        // set up the producer
        KafkaProducer<String, String> producer;
        try (InputStream props = Resources.getResource("producer.props").openStream()) {
            Properties properties = new Properties();
            properties.load(props);
            producer = new KafkaProducer<>(properties);
        }
        // -define .csv file in app
        String fileNameDefined = "/home/mapr/versions-payment-transactions-2016-05-07-05-00-00-2016-05-08-05-00-00-2b85e1ff-51a1-4af0-a080-b6e14e8c62b5.csv";
        ICsvListReader listReader = null;

        try {
                listReader = new CsvListReader(new FileReader(fileNameDefined), CsvPreference.STANDARD_PREFERENCE);
                
                listReader.getHeader(true); // skip the header (can't be used with CsvListReader)
                final CellProcessor[] processors = getProcessors();
                
                ArrayList<Object> list;
                ArrayList<HashMap<String, Object>> events = new ArrayList<HashMap<String, Object>>();
                Gson gson = new Gson();

                while( (list = (ArrayList<Object>) listReader.read(processors)) != null ) {
                        //System.out.println(String.format("lineNo=%s, rowNo=%s, customerList=%s", listReader.getLineNumber(),
                          //      listReader.getRowNumber(), customerList));
                    HashMap<String, Object> event = new HashMap<String, Object>();
                    HashMap<String, Object> object = ((HashMap<String, Object>) list.get(5));
                    HashMap<String, Object> objectChanges = ((HashMap<String, Object>) list.get(7));

                    long xid = (long) Math.floor(Math.random()*(Math.pow(10,18)));
                    event.put("xid", String.valueOf(xid));

                    ArrayList<HashMap<String, Object>> changeArr = new ArrayList<HashMap<String, Object>>();
                    HashMap<String, Object> change = new HashMap<String, Object>();
                    change.put("schema", "public");
                    change.put("table", "payment_transactions");

                    if(object != null) {
                        change.put("uuid", object.get("uuid"));
                    } else {
                        change.put("uuid", objectChanges.get("uuid"));
                    }

                    change.put("event", list.get(3));
                    change.put("event_ts", list.get(6));
                    change.put("whodunnit", list.get(4));
                    change.put("metadata", new HashMap<String, Object>());

                    if(object != null) {
                        change.put("object", object);
                    } else {
                        change.put("object", new HashMap<String, Object>());
                    }
                    
                    change.put("change_set", objectChanges);

                    changeArr.add(change);
                    event.put("change", changeArr);

                    producer.send(new ProducerRecord<String, String>(TOPIC,gson.toJson(event)));

                }
                System.out.println("Done");
                
                
        }
        finally {
                if( listReader != null ) {
                        listReader.close();
                }
        }

        System.out.println("Exiting");

    }

    private static CellProcessor[] getProcessors() {
            

        final CellProcessor[] processors = new CellProcessor[] { 
                new UniqueHashCode(), // id (must be unique) 0 
                new NotNull(), // item_Type 1
                new NotNull(), // item_id 2
                new NotNull(), //event 3
                new NotNull(), //whodunnit, 4
                new Optional(new ParseObjectDump()), //Object 5
                new ParseDate("yyyy-MM-dd HH:mm:ss"), // created at 6
                new Optional(new ParseObjectDump(new CondenseChangeArrays())) //object_changes 7
        };
        
        return processors;
    }

}

class ParseObjectDump extends CellProcessorAdaptor {
        
        public ParseObjectDump() {
            super();
        }
        
        public ParseObjectDump(CellProcessor next) {
            // this constructor allows other processors to be chained after ParseObjectDump
            super(next);
        }
        
        public Object execute(Object value, CsvContext context) {
            Yaml yaml = new Yaml();

            validateInputNotNull(value, context);  // throws an Exception if the input is null
            
            value = ((String) value).replaceFirst("!ruby/hash:ActiveSupport::HashWithIndifferentAccess", "");
            HashMap<String, Object> obj = (HashMap<String, Object>) yaml.load((String) value);

            return next.execute(obj, context);

        }
}

class CondenseChangeArrays extends ParseObjectDump {
        
        public CondenseChangeArrays() {
            super();
        }
        
        public CondenseChangeArrays(CellProcessor next) {
            // this constructor allows other processors to be chained after ParseObjectDump
            super(next);
        }
        
        public Object execute(Object value, CsvContext context) {

            HashMap<String, Object> hash = (HashMap<String, Object>) value;

            for(String key : hash.keySet()) {
                hash.put(key, ( (List<Object>) hash.get(key) ).get(1));
            }

            return next.execute(hash, context);

        }
}

