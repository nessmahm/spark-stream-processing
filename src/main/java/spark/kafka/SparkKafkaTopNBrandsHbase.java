package spark.kafka;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.ForeachFunction;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.sql.*;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.streaming.StreamingQuery;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;


public class SparkKafkaTopNBrandsHbase {


    public static void main(String[] args) throws Exception {


        if (args.length < 3) {
            System.err.println("Usage: SparkKafkaTopNBrands Hbase <bootstrap-servers> <subscribe-topics> <group-id>");
            System.exit(1);
        }

        String bootstrapServers = args[0];
        String topics = args[1];
        String groupId = args[2];
        int topN = 10;
        String hbaseTable = "brands";
        String columnFamily = "stats";

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
        /*
        * StreamingQuery query = brandCounts.orderBy(functions.col("count").desc())
                .limit(topN)
                .writeStream()
                .outputMode("complete")
                .format("console")
                .start();
                * */
        // Define HBase configuration
        Configuration hbaseConfig = HBaseConfiguration.create();
        System.out.println("started !!!!!!!");

        // Define the StreamingQuery with foreachBatch to write to HBase
        StreamingQuery hbaseQuery = brandCounts.orderBy(functions.col("count").desc())
                .limit(topN)
                .writeStream()
                .outputMode("complete")
                .foreachBatch((batchDF, batchId) -> {
                    System.out.println("Processing batch DF: " + batchDF);
                    System.out.println("Processing batch ID: " + batchId);
                    try (Connection connection = ConnectionFactory.createConnection(hbaseConfig);
                         Admin admin = connection.getAdmin()) {
                        // Create table with column families
                        TableDescriptor tableDescriptor = TableDescriptorBuilder.newBuilder(TableName.valueOf(hbaseTable))
                                .setColumnFamily(ColumnFamilyDescriptorBuilder.of(columnFamily))
                                .build();
                        System.out.println("Connecting");
                        Table table = connection.getTable(TableName.valueOf(hbaseTable));
                        try{
                            System.out.println("entred try block"+batchDF.count());
                            // Process each row in the batch
                            int index = 0; // Initialize a counter
                            for (Row row: batchDF.takeAsList(topN))  {
                                try {
                                    // Extract brand and count from the row
                                    System.out.println("processing row"+row);
                                    String brand = row.getString(row.fieldIndex("value"));
                                    long count = row.getLong(row.fieldIndex("count"));
                                    System.out.println("processing brand"+brand+"count: "+count);

                                    Put put = new Put(Bytes.toBytes(index));
                                    put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes("count"), Bytes.toBytes(Long.toString(count)));
                                    put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes("brand"), Bytes.toBytes(brand));

                                    // Write the new data directly to HBase, replacing the existing result
                                    table.put(put);

                                    System.out.println("Reading data...");
                                    Get g = new Get(Bytes.toBytes(index));
                                    Result r = table.get(g);
                                    System.out.println(Bytes.toString(r.getValue(Bytes.toBytes(columnFamily), Bytes.toBytes("brand"))));
                                    // Log success message for each updated brand
                                    System.out.println("Successfully replaced count for brand '" + brand + "' with new count: " + count);
                                    index+=1;
                                }
                                catch (Exception e) {
                                    // Log exceptions that occur during processing
                                    System.err.println("An error occurred while processing row: " + e.getMessage());
                                }
                            }

                        }
                        catch (Exception e) {
                            e.printStackTrace();
                            System.err.println("An error occurred while accessing HBase table: " + e.getMessage());

                        }
                    }

                    catch (Exception e) {
                        // Handle exceptions
                        System.err.println("An error occurred while accessing HBase table: " + e.getMessage());
                    }
                })
                .start();


        // Await termination

      //  query.awaitTermination();
        hbaseQuery.awaitTermination();
    }

}
