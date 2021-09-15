package main;

import producer.MyProducer;

import java.util.UUID;
import java.util.concurrent.ExecutionException;

public class ProduceNewOrderMain {

	public static void main (String[] args) throws ExecutionException, InterruptedException {

		try(var producer = new MyProducer()) {
			for (var i = 0; i < 10; i++) {

				producer.send("ECOMMERCE_NEW-ORDER", UUID.randomUUID().toString(), "Pedido");
				producer.send("ECOMMERCE_SEND-EMAIL", UUID.randomUUID().toString(), "E-mail");
			}
		}

	}

}
