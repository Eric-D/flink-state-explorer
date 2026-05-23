package com.test.explorer;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * KafkaSource → map → KafkaSink (exactly-once) job, used to produce a savepoint that contains real
 * Kafka connector state: source reader splits ("SourceReaderState"), source enumerator coordinator
 * state, and sink committer/writer state. Args: [bootstrapServers] [inputTopic] [outputTopic].
 */
public class KafkaJob {

    public static void main(String[] args) throws Exception {
        String bootstrap = args.length > 0 ? args[0] : "kafka:19092";
        String inputTopic = args.length > 1 ? args[1] : "explorer-input";
        String outputTopic = args.length > 2 ? args[2] : "explorer-output";

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);
        // Checkpointing populates source offsets and (for EXACTLY_ONCE) the sink transaction state.
        env.enableCheckpointing(1000);

        KafkaSource<String> source = KafkaSource.<String>builder()
                .setBootstrapServers(bootstrap)
                .setTopics(inputTopic)
                .setGroupId("explorer-test-group")
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .build();

        KafkaSink<String> sink = KafkaSink.<String>builder()
                .setBootstrapServers(bootstrap)
                .setRecordSerializer(KafkaRecordSerializationSchema.builder()
                        .setTopic(outputTopic)
                        .setValueSerializationSchema(new SimpleStringSchema())
                        .build())
                .setDeliveryGuarantee(DeliveryGuarantee.EXACTLY_ONCE)
                .setTransactionalIdPrefix("explorer-txn")
                // Flink's default (1h) exceeds the broker's transaction.max.timeout.ms (15min),
                // which fails InitProducerId; keep it under the broker max.
                .setProperty("transaction.timeout.ms", "600000")
                .build();

        DataStream<String> stream = env
                .fromSource(source, WatermarkStrategy.noWatermarks(), "Kafka Source")
                .uid("kafka-source");

        stream.map(String::toUpperCase).uid("upper")
                .sinkTo(sink).uid("kafka-sink");

        env.execute("Kafka Savepoint Test Generator");
    }
}
