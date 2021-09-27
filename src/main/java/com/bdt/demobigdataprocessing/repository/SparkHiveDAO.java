package com.bdt.demobigdataprocessing.repository;

import com.bdt.demobigdataprocessing.config.spark.MySparkConfig;
import org.apache.spark.sql.SparkSession;
import org.springframework.core.Ordered;
import org.springframework.core.annotation.Order;
import org.springframework.stereotype.Component;
import org.springframework.stereotype.Repository;

import javax.annotation.PostConstruct;


public class SparkHiveDAO {

    private static SparkSession sparkSession;


    public static void setupDAO(){
        sparkSession = SparkSession
                .builder()
                .appName("Java Spark Hive Persistence")
                //.config("hive.metastore.uris", "thrift://127.0.0.1:9083")
                .enableHiveSupport()
                .getOrCreate();


        sparkSession.sql("CREATE TABLE IF NOT EXISTS no_retweet( tweet VARCHAR(280)) ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' COLLECTION ITEMS TERMINATED BY '\n'");
        sparkSession.sql("SHOW TABLES").show();


    }


    public static void persistToHive(String tweet){

        sparkSession.sql("INSERT INTO no_retweet VALUES('"+tweet+"')");
        //sparkSession.sql("INSERT INTO no_retweet VALUES( 'Demo tweet')").show();
        sparkSession.sql("SELECT * FROM no_retweet").show();

    }
}
