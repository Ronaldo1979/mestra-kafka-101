package consumer;

import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.time.Duration;

import static java.util.Collections.singletonList;
import static utils.Constants.CONSUMER_CONFIG;

public class FraudCheckConsumer {

	public static void main (String[] args) throws InterruptedException {
		KafkaConsumer<String, String> consumer = new KafkaConsumer<>(CONSUMER_CONFIG(FraudCheckConsumer.class.getSimpleName()));
		consumer.subscribe(singletonList("ECOMMERCE_NEW-ORDER"));

		while (true) {
			ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));

			if (!records.isEmpty()) {

				System.out.println("Foram encontrados " + records.count() + " registros");

				for (var record: records) {
					System.out.println("================ ");
					System.out.println("Processando novo pedindo - CHECAGEM DE FRAUDE");
					System.out.println("CHAVE: " + record.key());
					System.out.println("VALOR: " + record.value());
					System.out.println("PARTITION: " + record.partition());
					System.out.println("OFFSET: " + record.offset());

					Thread.sleep(1000);

					System.out.println("ORDEM PROCESSADA COM SUCESSO!");
				}
			}

		}


	}
}
