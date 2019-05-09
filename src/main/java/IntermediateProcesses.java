import com.fasterxml.jackson.databind.JsonNode;
import org.apache.commons.collections.MultiMap;
import org.apache.commons.collections.map.MultiValueMap;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;

import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerConfig;

import java.util.Arrays;
import java.util.stream.Stream;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.protocol.types.Field;

import static java.util.stream.Collectors.toList;


public class IntermediateProcesses {
    public static void main(String[] args) throws Exception {
        String topicName1 = args[0];
        String topicName2 = args[1];
        String localHost = args[2];
        IntermediateProcesses.ConsumerThread consumerRunnable = new IntermediateProcesses.ConsumerThread(topicName1, topicName2, localHost, "50");
        consumerRunnable.start();
        String line = "";

        //consumerRunnable.getKafkaConsumer().wakeup();
        System.out.println("Stopping consumer .....");
        consumerRunnable.join();
    }

    private static class ConsumerThread extends Thread {
        private String topicName1;
        private String topicName2;
        private String localHost;
        private String groupId;
        private KafkaConsumer<String, String> kafkaConsumer;

        public ConsumerThread(String topicName1, String topicName2, String localHost, String groupId) {
            this.topicName1 = topicName1;
            this.topicName2 = topicName2;
            this.localHost = localHost;
            this.groupId = groupId;
        }

        public void run() {
            Properties configProperties = new Properties();
            Properties configPropertiesProducer = new Properties();
            configPropertiesProducer.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, localHost);
            configPropertiesProducer.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.ByteArraySerializer");
            configPropertiesProducer.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.connect.json.JsonSerializer");
            configProperties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, localHost);
            configProperties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
            configProperties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
            configProperties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
            configProperties.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
            configProperties.put(ConsumerConfig.CLIENT_ID_CONFIG, "simple");
            ObjectMapper objectMapper = new ObjectMapper();
            Producer producer = new KafkaProducer(configPropertiesProducer);
            kafkaConsumer = new KafkaConsumer<String, String>(configProperties);

            kafkaConsumer.subscribe(Arrays.asList(topicName1));
            MultiMap multiMap = new MultiValueMap();
            //Start processing messages
            try {

                while (true) {
                    ConsumerRecords<String, String> records = kafkaConsumer.poll(100);
                    for (ConsumerRecord<String, String> record : records) {
                        String eventIds = record.value().split(":")[1].split(",")[0];

                        String packageIds = record.value().split("packageId")[1].split(":")[1].split(",")[0];
                        multiMap.put(packageIds, eventIds);
                        PackageKafka packages = new PackageKafka(packageIds, multiMap.get(packageIds).toString());
                        JsonNode jsonNode = objectMapper.valueToTree(packages);
                        ProducerRecord<String, JsonNode> eventToSend = new ProducerRecord<String, JsonNode>(topicName2, jsonNode);
                        producer.send(eventToSend);
                    }
//                    for(Object key: multiMap.keySet()){
//                        PackageKafka packages = new PackageKafka(key.toString(), multiMap.get(key).toString());
//                        JsonNode jsonNode = objectMapper.valueToTree(packages);
//                        ProducerRecord<String, JsonNode> eventToSend = new ProducerRecord<String, JsonNode>(topicName2, jsonNode);
//                        System.out.println(eventToSend);
//                        producer.send(eventToSend);
//                    }
                }

            } catch (WakeupException ex) {
                System.out.println("Exception caught " + ex.getMessage());
            } finally {
                kafkaConsumer.close();
                System.out.println("After closing KafkaConsumer");
            }
        }

        public KafkaConsumer<String, String> getKafkaConsumer() {
            return this.kafkaConsumer;
        }
    }

}
