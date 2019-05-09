import java.sql.SQLOutput;
import java.text.SimpleDateFormat;
import java.util.*;
import java.sql.Timestamp;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.JsonNode;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import scala.math.Ordering;


public class ProducerKafka {
    private static final SimpleDateFormat sdf = new SimpleDateFormat("yyyy.MM.dd.HH.mm.ss");

    public static void main(String[] args) {
        String topicName = args[0];
        String localHost = args[1];
        Properties configProperties = new Properties();
        Timestamp timestamp = new Timestamp(System.currentTimeMillis());
        configProperties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, localHost);
        configProperties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.ByteArraySerializer");
        configProperties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.connect.json.JsonSerializer");
        ObjectMapper objectMapper = new ObjectMapper();
        long[] eventIdP1 = {1, 2, 3};
        long[] eventIdP2 = {2, 3, 4};
        long[] eventIdP3 = {4, 5, 2};

        PackagesKafka package1 = new PackagesKafka(1, eventIdP1, "package1");
        PackagesKafka package2 = new PackagesKafka(2, eventIdP2, "package2");
        PackagesKafka package3 = new PackagesKafka(3, eventIdP3, "package3");
        PackagesKafka package4 = new PackagesKafka(4, eventIdP1, "package1");
        PackagesKafka package5 = new PackagesKafka(5, eventIdP2, "package2");
        PackagesKafka package6 = new PackagesKafka(6, eventIdP3, "package3");
        ArrayList<PackagesKafka> packages = new ArrayList<PackagesKafka>();
        Producer producer = new KafkaProducer(configProperties);
        packages.add(package1);
        packages.add(package2);
        packages.add(package3);
        packages.add(package4);
        packages.add(package5);
        packages.add(package6);
        ProducerKafka obj = new ProducerKafka();
        Random rand = new Random();
        List<Integer> list = new ArrayList<>();
        int x = 0;
        while (x < packages.size()) {
            int randomNumber = rand.nextInt(packages.size());
            if (!list.contains(randomNumber)) {
                List<Long> packageEventIDS = new ArrayList<Long>();
                int i = 0;
                while (i < packages.get(randomNumber).getEventsIds().length) {
                    long eventID = obj.getRandomElement(packages.get(randomNumber).getEventsIds());

                    if (!packageEventIDS.contains(eventID)) {

                        packageEventIDS.add(eventID);
                        EventsKafka event = new EventsKafka(eventID, packages.get(randomNumber).getID(), sdf.format(timestamp), "The name of event is Event" + eventID);
                        JsonNode jsonNode = objectMapper.valueToTree(event);
                        ProducerRecord<String, JsonNode> eventToSend = new ProducerRecord<String, JsonNode>(topicName, jsonNode);
                        producer.send(eventToSend);

                    }
                    i = packageEventIDS.size();
                }

                list.add(randomNumber);
            }
            x = list.size();
        }


        producer.close();
    }

    public long getRandomElement(long[] list) {
        Random rand = new Random();
        return list[rand.nextInt(list.length)];
    }
}



