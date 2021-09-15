package consumer;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import utils.ConsumerRecordFunction;

import java.io.Closeable;
import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

public class MyConsumer implements Closeable {

	private final KafkaConsumer<String, String> consumer;
	private final ConsumerRecordFunction function;

	public MyConsumer (String groupId, ConsumerRecordFunction function, String topic) {
		Properties properties = CONSUMER_CONFIG(groupId);
		this.function = function;
		this.consumer = new KafkaConsumer<String, String>(properties);
		this.consumer.subscribe(Collections.singletonList(topic));
	}

	@Override
	public void close () {
		this.consumer.close();
	}

	public void run(Duration duration) {
		while (true) {
			ConsumerRecords<String, String> records = consumer.poll(duration);

			if (!records.isEmpty()) {
				System.out.println("Foram encontrados " + records.count() + " registros");
				for (var record: records) {
					this.function.consume(record);
				}
			}
		}
	}

	private static Properties CONSUMER_CONFIG(String groupId) {
		Properties properties = new Properties();
		properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
		properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
		properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
		properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
		properties.setProperty(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "1");
		return properties;
	}
}
