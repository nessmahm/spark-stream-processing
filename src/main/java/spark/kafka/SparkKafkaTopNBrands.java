package spark.kafka;

import java.util.Arrays;

import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.*;

import org.apache.spark.sql.streaming.StreamingQuery;
import scala.Tuple2;


public class SparkKafkaTopNBrands {


    public static void main(String[] args) throws Exception {


        if (args.length < 3) {
            System.err.println("Usage: SparkKafkaTopNBrands <bootstrap-servers> <subscribe-topics> <group-id>");
            System.exit(1);
        }
        System.out.println("hey !!!!!!!");

        String bootstrapServers = args[0];
        String topics = args[1];
        String groupId = args[2];
        int topN = 10;

        SparkSession spark = SparkSession
                .builder()
                .appName("SparkKafkaTopNBrands")
                .getOrCreate();

        // Create DataFrame representing the stream of input lines from Kafka
        Dataset<Row> df = spark
                .readStream()
                .format("kafka")
                .option("kafka.bootstrap.servers", bootstrapServers)
                .option("subscribe", topics)
                .option("kafka.group.id", groupId)
                .load();

        // Extract brand from the CSV data and count occurrences
        Dataset<Row> brandCounts = df.selectExpr("CAST(value AS STRING)")
                .as(Encoders.STRING())
                .flatMap((FlatMapFunction<String, String>) value -> {
                    String[] parts = value.split(",");
                    return Arrays.asList(parts[5]).iterator(); // Assuming brand is the 6th column
                }, Encoders.STRING())
                .groupBy("value").count();

        // Output top N brands to console
        StreamingQuery query = brandCounts.orderBy(functions.col("count").desc())
                .limit(topN)
                .writeStream()
                .outputMode("complete")
                .format("console")
                .start();

        query.awaitTermination();
    }
}
