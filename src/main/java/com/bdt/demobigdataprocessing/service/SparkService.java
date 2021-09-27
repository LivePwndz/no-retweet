package com.bdt.demobigdataprocessing.service;

import com.bdt.demobigdataprocessing.config.spark.MySparkConfig;
import com.bdt.demobigdataprocessing.repository.SparkHiveDAO;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.annotation.Order;
import org.springframework.stereotype.Component;
import scala.Tuple2;

@Component()
@Order(5)
public class SparkService {



    public void noRetreets() {
        JavaPairDStream<String, String> kafkaRecordStream = MySparkConfig.getSparkStream()
                .mapToPair(kr -> new Tuple2<>(kr.key(), kr.value()));

        kafkaRecordStream
                .map(Tuple2::_2).filter(t -> !t.startsWith("RT")).foreachRDD(rdd -> {
            if (rdd.isEmpty()) return;
            rdd.collect().stream().forEach(tweet->{
                SparkHiveDAO.persistToHive(tweet);
                System.out.println("In loop: "+tweet);
            });
//
        });



        MySparkConfig.streamingContext.start();


    }
}
