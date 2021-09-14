package consumer;

import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.time.Duration;
import java.util.regex.Pattern;

import static utils.Constants.CONSUMER_CONFIG;

public class LogServiceConsumer {

	public static void main (String[] args) {

		var consumer = new KafkaConsumer<String, String>(CONSUMER_CONFIG(LogServiceConsumer.class.getSimpleName()));
		consumer.subscribe(Pattern.compile("ECOMMERCE.*"));

		while (true) {
			ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));

			if (!records.isEmpty()) {

				System.out.println("Foram encontrados " + records.count() + " registros");

				for (var record: records) {
					System.out.println("================");
					System.out.println("LOGANDO O TÓPICO: " + record.topic());
					System.out.println("CHAVE: " + record.key());
					System.out.println("VALOR: " + record.value());
					System.out.println("PARTITION: " + record.partition());
					System.out.println("OFFSET: " + record.offset());
					System.out.println("================");
					System.out.println("\n");
				}
			}
		}

	}

}
