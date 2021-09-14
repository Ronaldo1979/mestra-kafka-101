package producer;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.io.Closeable;
import java.util.concurrent.ExecutionException;

import static utils.Constants.PRODUCER_CONFIG;

public class MyProducer implements Closeable {

	private final KafkaProducer<String, String> producer;

	public MyProducer () {
		this.producer = new KafkaProducer<String, String>(PRODUCER_CONFIG());
	}

	public void send(String topic, String key, String value, Callback callback) throws ExecutionException, InterruptedException {
		var orderRecord = new ProducerRecord<>(topic, key, value);
		producer.send(orderRecord, callback).get();
	}

	public void send(String topic, String key, String value) throws ExecutionException, InterruptedException {
		Callback callback = new Callback() {
			@Override
			public void onCompletion (RecordMetadata metadata, Exception exception) {
				if (exception != null) {
					exception.printStackTrace();
					return;
				}

				System.out.println("=============== ");
				System.out.println("TOPIC: " + metadata.topic());
				System.out.println("PARTITION: " + metadata.partition());
				System.out.println("OFFSET: " + metadata.offset());
				System.out.println("TIMESTAMP: " + metadata.timestamp());
			}
		};
		this.send(topic, key, value, callback);
	}

	@Override
	public void close () {
		this.producer.close();
	}
}
