#### Project Overview

This project is designed to analyze e-commerce sales data in real-time using Apache Kafka, Apache Spark, and HBase. The data stream is ingested from Kafka, processed with Spark to extract and count brand occurrences, and the top N brands are then stored in HBase for further analysis. The results are also displayed in real-time, offering insights into brand trends.

---

#### Project Structure

1. **SparkKafkaTopNBrands**: 
   - Reads a stream of messages from a Kafka topic.
   - Extracts the brand information from the data and counts occurrences.
   - Outputs the top N brands in real-time to the console.

2. **SparkKafkaTopNBrandsHbase** : 
   - Similar to the first file, but also writes the top N brands to HBase for persistent storage.
   - The data is written to a specified HBase table, and the system ensures that each batch of top N brands is updated.
   - This file provides fault tolerance and ensures that data is consistently stored in HBase.

3. **Kafka Producer**:
   - This is used to simulate data input into the Kafka topics that Spark consumes for real-time processing.
   - It generates e-commerce transaction data with brand details and sends it to the Kafka topic.

---

#### Prerequisites

- **Apache Kafka**: For streaming real-time data.
- **Apache Spark**: For processing the streaming data in real-time.
- **HBase**: For storing the processed data.
- **Java**: The code is written in Java and uses Apache Spark's Java API.

---

#### How to Run

1. **Start Kafka**: 
   - Set up Kafka with the desired bootstrap server configuration.
   - Create topics to be consumed by Spark.
   
2. **Start HBase**: 
   - Ensure HBase is running and the table `brands` exists.

3. **Start the Spark Application**:
   - Compile and run the `SparkKafkaTopNBrands` or `SparkKafkaTopNBrandsHbase` class, passing the appropriate Kafka bootstrap server, topics, and consumer group ID as arguments:
     ```bash
     java -jar SparkKafkaTopNBrands.jar <bootstrap-servers> <topics> <group-id>
     ```
   
   - The Spark job will process the stream and output the results either to the console (for `SparkKafkaTopNBrands`) or to HBase (for `SparkKafkaTopNBrandsHbase`).

4. **Kafka Producer**:
   - You can also run a Kafka producer to send synthetic data to the Kafka topic:
     ```bash
     java -jar KafkaProducer.jar <bootstrap-servers> <topic>
     ```

---
