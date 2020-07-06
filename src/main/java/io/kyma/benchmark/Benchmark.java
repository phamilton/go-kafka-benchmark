package io.kyma.benchmark;


import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.clients.producer.*;

import picocli.CommandLine;
import picocli.CommandLine.Option;
import picocli.CommandLine.Parameters;


import java.io.File;
import java.util.Arrays;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.ExecutionException;

public class Benchmark {

    static boolean done = false;
    static int msgCount = 0;

    @Option(names = "-brokers", description = "broker addresses", defaultValue = "localhost:9092")
    String brokers;

    @Option(names = "-topic", description = "topic", defaultValue = "default.commerce")
    String topic;

    @Option(names = "-msgsize", description = "message size", defaultValue = "64")
    int msgsize;

    @Option(names = "-numMessages", description = "number of messages", defaultValue = "100000")
    int numMessages;

    @Option(names = "-mode", description = "consumer or producer", defaultValue = "consumer")
    String mode;

    @Option(names = "-username", description = "username", defaultValue = "")
    String username;

    @Option(names = "-password", description = "password", defaultValue = "")
    String password;

    @Option(names = "-sasl", description = "use sasl", defaultValue = "false")
    Boolean sasl;

    @Option(names = "-trustStorePassword", description = "trust store password", defaultValue = "")
    String trustStorePassword;

    @Option(names = "--help", usageHelp = true, description = "display this help and exit")
    boolean help;

    public static void main(String[] args) throws InterruptedException, ExecutionException {
        Benchmark benchmark = new Benchmark();
        CommandLine cmd = new CommandLine(benchmark);

        CommandLine.ParseResult parseResult = cmd.parseArgs(args);

        // Did user request usage help (--help)?
        if (cmd.isUsageHelpRequested()) {
            cmd.usage(cmd.getOut());
            return;
        }

        if (benchmark.getMode().equals("consumer")) {
            System.out.println("Starting consumer");
            benchmark.benchmarkConsumer();
        } else {
            System.out.println("Starting producer");
            benchmark.benchmarkProducer();
        }
    }

    public String getMode() {
        return mode;
    }

    public void benchmarkProducer() {
        Properties props = new Properties();
        props.put("bootstrap.servers", brokers);
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        if (sasl) {
            props.put("security.protocol", "SASL_SSL");
            props.put("sasl.mechanism", "PLAIN");
            props.put("sasl.jaas.config", "org.apache.kafka.common.security.plain.PlainLoginModule required username=\"" + username + "\" password=\"" + password + "\";");
            props.put("ssl.truststore.location", "kafka.client.truststore.jks");
            props.put("ssl.truststore.password", trustStorePassword);
            props.put("ssl.endpoint.identification.algorithm", "");
        }

        Producer<String, String> producer = new KafkaProducer<>(props);

        long startTime = System.currentTimeMillis();
        for (int i = 0; i < numMessages; i++)
            producer.send(new ProducerRecord<String, String>(topic, Integer.toString(i), "abcdefg"), new Callback() {

                @Override
                public void onCompletion(RecordMetadata m, Exception e) {
                    if (e != null) {
                        e.printStackTrace();
                    } else {
                        msgCount++;
                        if (msgCount >= numMessages) {
                            long endTime = System.currentTimeMillis();
                            System.out.println(String.valueOf(Double.valueOf(msgCount) / (Double.valueOf(endTime - startTime) / 1000)) + " msgs/s");

                            done = true;
                        }
                    }
                }
            });

        while (!done) {
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }

        producer.flush();
        producer.close();
    }

    public void benchmarkConsumer() {
        // Load properties from a local configuration file
        // Create the configuration file (e.g. at '$HOME/.confluent/java.config') with configuration parameters
        // to connect to your Kafka cluster, which can be on your local host, Confluent Cloud, or any other cluster.
        // Follow these detailed instructions to properly create this file: https://github.com/confluentinc/configuration-templates/tree/master/README.md
        //final Properties props = loadConfig(args[0]);

        // Add additional properties.
        String groupId = UUID.randomUUID().toString();
        Properties props = new Properties();
        props.put("bootstrap.servers", brokers);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        if (sasl) {
            props.put("security.protocol", "SASL_SSL");
            props.put("sasl.mechanism", "PLAIN");
            props.put("sasl.jaas.config", "org.apache.kafka.common.security.plain.PlainLoginModule required username=\"" + username + "\" password=\"" + password + "\";");
            props.put("ssl.truststore.location", "kafka.client.truststore.jks");
            props.put("ssl.truststore.password", trustStorePassword);
            props.put("ssl.endpoint.identification.algorithm", "");
        }


        final Consumer<String, String> consumer = new KafkaConsumer<String, String>(props);
        consumer.subscribe(Arrays.asList(topic));

        long startTime = System.currentTimeMillis();
        try {
            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(100);

                for (ConsumerRecord<String, String> record : records) {
                    String key = record.key();
                    String value = record.value();
                    msgCount++;
                    if (msgCount >= numMessages) {
                        long endTime = System.currentTimeMillis();
                        System.out.println(String.valueOf(Double.valueOf(msgCount) / (Double.valueOf(endTime - startTime) / 1000)) + " msgs/s");
                        return;
                    }
                }
            }
        } finally {
            consumer.close();
        }
    }
}


