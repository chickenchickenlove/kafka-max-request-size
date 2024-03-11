package org.example;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.shaded.com.fasterxml.jackson.databind.ser.std.StringSerializer;
import org.testcontainers.utility.DockerImageName;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.Assertions.*;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class MainTest {

    String kafkaImage = "confluentinc/cp-enterprise-kafka:6.0.1";
    String topicName = "test-topic";
    KafkaContainer kafkaContainer;
    String kafkaBootstrapServer;


    @BeforeAll
    public void setUp() throws InterruptedException{
        // Container + AdminClient
        setUpKafkaContainer();
    }


    private void setUpKafkaContainer() {
        final DockerImageName kafkaImageName = DockerImageName
                .parse(kafkaImage)
                .asCompatibleSubstituteFor("confluentinc/cp-kafka");

        kafkaContainer = new KafkaContainer(kafkaImageName)
                .withEmbeddedZookeeper();

        kafkaContainer.start();
        kafkaBootstrapServer = kafkaContainer.getBootstrapServers();
        printReadyLog("kafka container");
    }

    private void printReadyLog(String name) {
        System.out.printf("%s is ready for testing.\n", name);
    }

    List<ProducerRecord<String, String>> getRecords(String topicName, int recordCount, int size) {
        List<ProducerRecord<String, String>> records = new ArrayList<>();

        for (int i = 0; i < recordCount; i++) {
            records.add(getSingleRecord(topicName, size));
        }
        return records;
    }

    ProducerRecord<String, String> getSingleRecord(String topicName, int size) {
        // 2MB ; 2 * 1024 * 1024 (Char is 2bytes in java)
        char[] chars = new char[1 * size * size];
        Arrays.fill(chars, 'a');

        final String value = new String(chars);
        return new ProducerRecord<>(topicName,
                null,
                value);
    }

    KafkaProducer<String, String> getProducer() {
        final Properties props = new Properties();
        props.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaBootstrapServer);
        // Set max.request.size = 1MB
        props.setProperty(ProducerConfig.MAX_REQUEST_SIZE_CONFIG, "1048576");
        // Set Buffer Size = 32MB
        props.setProperty(ProducerConfig.BUFFER_MEMORY_CONFIG, "33554432");
        props.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        return new KafkaProducer<>(props);
    }


    @Test
    void should_fail_with_bigSingleRecord() {
        // Given
        KafkaProducer<String, String> producer = getProducer(); // max.request.size = 1MB
        List<ProducerRecord<String, String>> records = getRecords(topicName, 1, 1024);// Single Record 2MB
        ProducerRecord<String, String> bigRecord = records.get(0);

        // When + Then
        assertThatThrownBy(() -> producer.send(bigRecord).get(10, TimeUnit.SECONDS))
                .isInstanceOf(ExecutionException.class)
                .hasMessageContaining("max.request.size");
    }

    @Test
    void should_success_with_smallRecords() throws ExecutionException, InterruptedException, TimeoutException {
        // Given
        int recordCount = 100;
        KafkaProducer<String, String> producer = getProducer(); // max.request.size = 1MB
        // Single Record Size : 130KB, Total Records Size > 1MB. (130KB * 100)
        List<ProducerRecord<String, String>> records = getRecords(topicName, recordCount, 256);

        // When
        List<RecordMetadata> results = new ArrayList<>();
        for (ProducerRecord<String, String> record : records) {
            results.add(producer.send(record).get(10, TimeUnit.SECONDS));
        }

        // Then
        assertThat(results.size()).isEqualTo(recordCount);
    }
}