/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package spark.streaming;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

public class Producer extends Thread {
	private final KafkaProducer<Integer, String> producer;
	private final String topic;
	private final Boolean isAsync;
	private String brokers = "bigdata04.nebuinfo.com:9092,bigdata05.nebuinfo.com:9092,bigdata06.nebuinfo.com:9092";

	public Producer(String topic, Boolean isAsync) {
		Properties props = new Properties();
		props.put("bootstrap.servers", brokers);
		props.put("client.id", "DemoProducer");
		props.put("key.serializer", "org.apache.kafka.common.serialization.IntegerSerializer");
		props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		producer = new KafkaProducer<>(props);
		this.topic = topic;
		this.isAsync = isAsync;
	}

	public void run() {
		int messageNo = 1;
		while (true) {
			//Utils.sleep(100);
			String messageStr ="{\"createTime\":\"2017-11-13 16:35:41\",\"status\":0,\"captureTime\":\"2017-11-13 04:35:39\",\"longitude\":\"120.355561\"," +
					"\"latitude\":\"30.693477\",\"firstTime\":\"2017-11-12 04:35:39\",\"lastTime\":\"2017-11-13 04:35:39\",\"moduleMac\":\"185F23BD924C\"," +
					"\"imsi\":\"460038624780"+Math.round(Math.random()*10)+"\",\"isp\":\"5\",\"distance\":\"1\",\"snCode\":\"EN1801E116480362\",\"manufacturerCode\":\"79094740\"}";
			long startTime = System.currentTimeMillis();
			if (isAsync) { // Send asynchronously
				producer.send(new ProducerRecord<>(topic,
						messageNo,
						messageStr), new DemoCallBack(startTime, messageNo, messageStr));
			} else { // Send synchronously
				try {
					producer.send(new ProducerRecord<>(topic,
							messageNo,
							messageStr)).get();
					System.out.println("Sent message: (" + messageNo + ", " + messageStr + ")");
				} catch (InterruptedException | ExecutionException e) {
					e.printStackTrace();
				}
			}
			++messageNo;

		}
	}

	public static void main(String[] args) {
		boolean isAsync = args.length == 0 || !args[0].trim().equalsIgnoreCase("sync");
		Producer producerThread = new Producer("scanEnding", isAsync);
		producerThread.start();
	}

}

class DemoCallBack implements Callback {

	private final long startTime;
	private final int key;
	private final String message;

	public DemoCallBack(long startTime, int key, String message) {
		this.startTime = startTime;
		this.key = key;
		this.message = message;
	}

	/**
	 * A callback method the user can implement to provide asynchronous handling of request completion. This method will
	 * be called when the record sent to the server has been acknowledged. Exactly one of the arguments will be
	 * non-null.
	 *
	 * @param metadata  The metadata for the record that was sent (i.e. the partition and offset). Null if an error
	 *                  occurred.
	 * @param exception The exception thrown during processing of this record. Null if no error occurred.
	 */
	public void onCompletion(RecordMetadata metadata, Exception exception) {
		long elapsedTime = System.currentTimeMillis() - startTime;
		if (metadata != null) {
			System.out.println(
					"message(" + key + ", " + message + ") sent to partition(" + metadata.partition() +
							"), " +
							"offset(" + metadata.offset() + ") in " + elapsedTime + " ms");
			if (key==100000) {
				System.exit(0);
			}
		} else {
			exception.printStackTrace();
		}
	}
}
