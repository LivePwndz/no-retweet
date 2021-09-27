package com.bdt.demobigdataprocessing;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka010.ConsumerStrategies;
import org.apache.spark.streaming.kafka010.KafkaUtils;
import org.apache.spark.streaming.kafka010.LocationStrategies;
import scala.Tuple2;
import org.apache.spark.streaming.*;

import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

public class SparkService {

    private JavaInputDStream<ConsumerRecord<String, String>> messages;
    private JavaStreamingContext streamingContext;





    SparkService() {
        SparkConf sparkConf = new SparkConf();
        sparkConf.setAppName("WordCountingApp");
        sparkConf.setMaster("local[4]");
        sparkConf.set("spark.sql.catalogImplementation","hive");


        // spark.master should be set as local[n], n > 1 in local mode if you have receivers



        streamingContext = new JavaStreamingContext(
                sparkConf, Durations.seconds(1));





        Map<String, Object> kafkaParams = new HashMap<>();
        kafkaParams.put("bootstrap.servers", "localhost:9092");
        kafkaParams.put("key.deserializer", StringDeserializer.class);
        kafkaParams.put("value.deserializer", StringDeserializer.class);
        kafkaParams.put("group.id", "use_a_separate_group_id_for_each_stream");
        kafkaParams.put("auto.offset.reset", "latest");
        kafkaParams.put("enable.auto.commit", false);
        Collection<String> topics = Arrays.asList("quickstart-events");

        messages =
                KafkaUtils.createDirectStream(
                        streamingContext,
                        LocationStrategies.PreferConsistent(),
                        ConsumerStrategies.<String, String>Subscribe(topics, kafkaParams));
    }

    void process() {
        JavaPairDStream<String, String> results = messages
                .mapToPair(
                        record ->new Tuple2<>(record.key(), record.value())
                );

        JavaDStream<String> lines = results
                .map(
                        tuple2 -> tuple2._2()
                );
        JavaDStream<String> words = lines
                .flatMap(
                        x -> Arrays.asList(x.split("\\s+")).iterator()
                );


        JavaPairDStream<String, Integer> wordCounts = words
                .mapToPair(
                        s -> new Tuple2<>(s, 1)
                ).reduceByKey(
                        (i1, i2) -> i1 + i2

                );



        SparkSession spark = SparkSession
                .builder()
                .appName("Java Spark Hive Example")
                //.config("spark.sql.warehouse.dir", warehouseLocation)
                //.config("hive.metastore.uris", "thrift://127.0.0.1:9083")
                .enableHiveSupport()
                .getOrCreate();




        //spark.sql("CREATE TABLE IF NOT EXISTS word_freq( word VARCHAR(3), count int) STORED AS TEXTFILE");
        //spark.sql("CREATE TABLE IF NOT EXISTS word_freq( word VARCHAR(3), count int) ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' COLLECTION ITEMS TERMINATED BY '\n'");
        spark.sql("SHOW TABLES").show();
        spark.sql("INSERT INTO word_freq VALUES( 'the', 50)").show();
        spark.sql("SELECT * FROM word_freq").show();


      wordCounts.foreachRDD((rdd)->{
          if(!rdd.isEmpty()){
              rdd.foreach(t->{
                  String word = t._1;
                  int count = t._2;

                  //spark.sql("INSERT INTO word_freq VALUES (word, count)").show();
              });
          }
      });








        wordCounts.print();
        //streamingContext.start();

        try {
            streamingContext.start();
            streamingContext.awaitTermination();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }



    }
}
