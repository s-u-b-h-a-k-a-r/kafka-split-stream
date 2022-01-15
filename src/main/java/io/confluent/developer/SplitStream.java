package io.confluent.developer;

import io.confluent.developer.avro.ActingEvent;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Branched;

import java.io.FileInputStream;
import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

public class SplitStream {
    public Topology buildTopology(Properties props) {
        StreamsBuilder builder = new StreamsBuilder();
        String inputTopic = props.getProperty("input.topic.name");
        builder.<String, ActingEvent>stream(inputTopic)
                .split()
                .branch((key, appearance) -> "drama".equals(appearance.getGenre()),
                        Branched.withConsumer(ks -> ks.to(props.getProperty("output.drama.topic.name"))))
                .branch((key, appearance) -> "fantasy".equals(appearance.getGenre()),
                        Branched.withConsumer(ks -> ks.to(props.getProperty("output.fantasy.topic.name"))))
                .branch((key, appearance) -> true,
                        Branched.withConsumer(ks -> ks.to(props.getProperty("output.other.topic.name"))));
        return builder.build();
    }

    public void createTopics(Properties props) {
        try (AdminClient adminClient = AdminClient.create(props)) {
            List<NewTopic> topics = new ArrayList<>();
            topics.add(new NewTopic(
                    props.getProperty("input.topic.name"),
                    Integer.parseInt(props.getProperty("input.topic.partitions")),
                    Short.parseShort(props.getProperty("input.topic.replication.factor"))));

            topics.add(new NewTopic(
                    props.getProperty("output.drama.topic.name"),
                    Integer.parseInt(props.getProperty("output.drama.topic.partitions")),
                    Short.parseShort(props.getProperty("output.drama.topic.replication.factor"))));

            topics.add(new NewTopic(
                    props.getProperty("output.fantasy.topic.name"),
                    Integer.parseInt(props.getProperty("output.fantasy.topic.partitions")),
                    Short.parseShort(props.getProperty("output.fantasy.topic.replication.factor"))));

            topics.add(new NewTopic(
                    props.getProperty("output.other.topic.name"),
                    Integer.parseInt(props.getProperty("output.other.topic.partitions")),
                    Short.parseShort(props.getProperty("output.other.topic.replication.factor"))));

            adminClient.createTopics(topics);
        }
    }

    public static void main(String[] args) throws IOException {
        if (args.length < 1) {
            throw new IllegalArgumentException("This program takes one argument: the path to an environment configuration file.");
        }
        SplitStream ss = new SplitStream();
        Properties props = loadProperties(args[0]);
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, SpecificAvroSerde.class);
        Topology topology = ss.buildTopology(props);
        ss.createTopics(props);
        final KafkaStreams kafkaStreams = new KafkaStreams(topology, props);
        CountDownLatch latch = new CountDownLatch(1);
        Runtime.getRuntime().addShutdownHook(
                new Thread(() -> {
                    kafkaStreams.close(Duration.ofSeconds(5));
                    latch.countDown();
                })
        );
        try {
            kafkaStreams.start();
            latch.await();
        } catch (Throwable e) {
            System.exit(1);
        }
        System.exit(0);
    }

    private static Properties loadProperties(String fileName) throws IOException {
        Properties props = new Properties();
        try (FileInputStream inputStream = new FileInputStream(fileName)) {
            props.load(inputStream);
        }
        return props;
    }
}
