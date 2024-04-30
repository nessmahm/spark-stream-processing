package spark.kafka;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.Properties;

public class KafkaCSVProducer {

    public static void main(String[] args) {

        if(args.length == 0){
            System.out.println("Entrer le nom du topic");
            return;
        }
        String topicName = args[0].toString();
        final String CSV_FILE_PATH = "ecommerce.csv";
        final String BOOTSTRAP_SERVERS = "localhost:9092";
        // Creer une instance de proprietes pour acceder aux configurations du producteur
        Properties props = new Properties();

        // Assigner l'identifiant du serveur kafka
        props.put("bootstrap.servers", "localhost:9092");

        // Definir un acquittement pour les requetes du producteur
        props.put("acks", "all");

        // Si la requete echoue, le producteur peut reessayer automatiquemt
        props.put("retries", 0);

        // Specifier la taille du buffer size dans la config
        props.put("batch.size", 16384);

        // buffer.memory controle le montant total de memoire disponible au producteur pour le buffering
        props.put("buffer.memory", 33554432);

        props.put("key.serializer",
                "org.apache.kafka.common.serialization.StringSerializer");

        props.put("value.serializer",
                "org.apache.kafka.common.serialization.StringSerializer");

        Producer<String, String> producer = new KafkaProducer
                <String, String>(props);

        try (BufferedReader br = new BufferedReader(new FileReader(CSV_FILE_PATH))) {
            String line;
            while ((line = br.readLine()) != null) {
                // Assuming CSV fields are comma-separated, you may need to adjust this based on your CSV format
                String[] fields = line.split(",");
                StringBuilder messageValue = new StringBuilder();
                for (String field : fields) {
                    messageValue.append(field).append(", ");
                }
                // Remove the trailing comma and space
                String value = messageValue.substring(0, messageValue.length() - 2);
                ProducerRecord<String, String> record = new ProducerRecord<>(topicName, null, value);
                producer.send(record);
                Thread.sleep(100); // Simulate streaming with some delay
            }
        } catch (IOException | InterruptedException e) {
            e.printStackTrace();
        } finally {
            producer.close();
        }
    }
}
