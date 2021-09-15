package service;

import org.apache.kafka.clients.consumer.ConsumerRecord;

public class FraudCheckService {

	public void process (ConsumerRecord<String, String> record) {
		System.out.println("================ ");
		System.out.println("Processando novo pedindo - CHECAGEM DE FRAUDE");
		System.out.println("CHAVE: " + record.key());
		System.out.println("VALOR: " + record.value());
		System.out.println("PARTITION: " + record.partition());
		System.out.println("OFFSET: " + record.offset());

		try {
			Thread.sleep(1000);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}

		System.out.println("ORDEM PROCESSADA COM SUCESSO!");
	}

}
