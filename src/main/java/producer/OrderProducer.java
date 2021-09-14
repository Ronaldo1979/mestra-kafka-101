package producer;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.UUID;
import java.util.concurrent.ExecutionException;

import static utils.Constants.PRODUCER_CONFIG;

public class OrderProducer {

	public static void main (String[] args) throws ExecutionException, InterruptedException {
		KafkaProducer<String, String> producer = new KafkaProducer<>(PRODUCER_CONFIG());
		var record = new ProducerRecord<>("ECOMMERCE_NEW-ORDER", UUID.randomUUID().toString(), "valor");
//		Callback callback = (data, ex) -> {
//			if (ex != null) {
//				ex.printStackTrace();
//				return;
//			}
//
//			System.out.println(" =============== ");
//			System.out.println("TOPIC: " + data.topic());
//			System.out.println("PARTITION: " + data.partition());
//			System.out.println("OFFSET: " + data.offset());
//			System.out.println("TIMESTAMP: " + data.timestamp());
//		};

		Callback callback = new Callback() {
			@Override
			public void onCompletion (RecordMetadata metadata, Exception exception) {
				if (exception != null) {
					exception.printStackTrace();
					return;
				}

				System.out.println(" =============== ");
				System.out.println("TOPIC: " + metadata.topic());
				System.out.println("PARTITION: " + metadata.partition());
				System.out.println("OFFSET: " + metadata.offset());
				System.out.println("TIMESTAMP: " + metadata.timestamp());
			}
		};

		producer.send(record, callback).get();
	}

}
