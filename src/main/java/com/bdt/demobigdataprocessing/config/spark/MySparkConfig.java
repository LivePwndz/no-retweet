package com.bdt.demobigdataprocessing.config.spark;

import com.bdt.demobigdataprocessing.repository.SparkHiveDAO;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.spark.SparkConf;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka010.ConsumerStrategies;
import org.apache.spark.streaming.kafka010.KafkaUtils;
import org.apache.spark.streaming.kafka010.LocationStrategies;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;


public class MySparkConfig {

    private static JavaInputDStream<ConsumerRecord<String, String>> sparkStream;
    public static JavaStreamingContext streamingContext = createStreamingContext();


    private MySparkConfig() {
    }

    public static synchronized JavaInputDStream<ConsumerRecord<String, String>> getSparkStream() {
        if (sparkStream == null) {
            sparkStream = createSparkStream();
            SparkHiveDAO.setupDAO();
        }

        return sparkStream;
    }




    private static JavaStreamingContext createStreamingContext() {
        SparkConf sparkConf = new SparkConf();
        sparkConf.setAppName("WordCountingApp");
        sparkConf.setMaster("local[4]");
        sparkConf.set("spark.sql.catalogImplementation", "hive");

        return new JavaStreamingContext(
                sparkConf, Durations.seconds(1));
    }

    private static JavaInputDStream<ConsumerRecord<String, String>> createSparkStream() {

        Map<String, Object> kafkaParams = new HashMap<>();
        kafkaParams.put("bootstrap.servers", "localhost:9092");
        kafkaParams.put("key.deserializer", StringDeserializer.class);
        kafkaParams.put("value.deserializer", StringDeserializer.class);
        kafkaParams.put("group.id", "use_a_separate_group_id_for_each_stream");
        kafkaParams.put("auto.offset.reset", "latest");
        kafkaParams.put("enable.auto.commit", false);
        Collection<String> topics = Collections.singletonList("quickstart-events");


        sparkStream =
                KafkaUtils.createDirectStream(
                        streamingContext,
                        LocationStrategies.PreferConsistent(),
                        ConsumerStrategies.Subscribe(topics, kafkaParams));


        return sparkStream;
    }
}
