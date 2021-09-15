package utils;

import org.apache.kafka.clients.consumer.ConsumerRecord;

@FunctionalInterface
public interface ConsumerRecordFunction {

	void consume(ConsumerRecord<String, String> record);

}
