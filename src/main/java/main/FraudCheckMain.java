package main;

import service.FraudCheckService;
import consumer.MyConsumer;
import utils.ConsumerRecordFunction;

import java.time.Duration;

public class FraudCheckMain {

	public static void main (String[] args) {

		FraudCheckService fraudCheckService = new FraudCheckService();

		String groupId = fraudCheckService.getClass().getSimpleName();
		ConsumerRecordFunction process = fraudCheckService::process;

		try(var myConsumer = new MyConsumer(groupId, process)) {
			myConsumer.run(Duration.ofMillis(1000));
		}
	}
}
