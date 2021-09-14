package consumer;

import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.time.Duration;
import java.util.Collections;

import static utils.Constants.CONSUMER_CONFIG;

public class EmailServiceConsumer {

	public static void main (String[] args) throws InterruptedException {
		var consumer = new KafkaConsumer<String, String>(CONSUMER_CONFIG(EmailServiceConsumer.class.getSimpleName()));
		consumer.subscribe(Collections.singletonList("ECOMMERCE_SEND-EMAIL"));

		while (true) {
			ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));

			if (!records.isEmpty()) {

				System.out.println("Foram encontrados " + records.count() + " registros");

				for (var record: records) {
					System.out.println("================ ");
					System.out.println("Enviando e-mail para novo pedido");
					System.out.println("CHAVE: " + record.key());
					System.out.println("VALOR: " + record.value());
					System.out.println("PARTITION: " + record.partition());
					System.out.println("OFFSET: " + record.offset());

					Thread.sleep(1500);

					System.out.println("EMAIL ENVIADO COM SUCESSO!");
				}
			}

		}
	}
}
