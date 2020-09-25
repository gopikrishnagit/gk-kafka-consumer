package ind.gopi.codelabs.kafka.consumer.service;

import ind.gopi.codelabs.kafka.consumer.config.InputProperties;
import ind.gopi.codelabs.kafka.consumer.config.OutputProperties;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Service;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;

@Slf4j
@Service
public class FileWriterService
{
	@Autowired
	private OutputProperties outputProperties;

	@Autowired
	private InputProperties inputProperties;

	@KafkaListener(topics = "hermes-error-log",
			containerFactory = "errorLogKafkaListenerContainerFactory")
	public void listenErrorLog(@Payload String message,
			@Header(KafkaHeaders.RECEIVED_TOPIC) String topic,
			@Header(KafkaHeaders.RECEIVED_PARTITION_ID) int partition,
			@Header(KafkaHeaders.OFFSET) int offset)
	{
		log.info("Received message from topic={}, partition={}, offset={}",
				topic, partition, offset);
		if (message.contains(inputProperties.getSearchPatternInMessage())) {
			log.info(
					"Processing message from topic={}, partition={}, offset={}",
					topic, partition, offset);
			writeToFile(message, topic, partition, offset);
		}
	}

	@KafkaListener(topics = "hermes-message-log",
			containerFactory = "messageLogKafkaListenerContainerFactory")
	public void listenMessageLog(@Payload String message,
			@Header(KafkaHeaders.RECEIVED_TOPIC) String topic,
			@Header(KafkaHeaders.RECEIVED_PARTITION_ID) int partition,
			@Header(KafkaHeaders.OFFSET) int offset)
	{
		log.info("Received message from topic={}, partition={}, offset={}",
				topic, partition, offset);
		if (message.contains(inputProperties.getSearchPatternInMessage())) {
			log.info(
					"Processing message from topic={}, partition={}, offset={}",
					topic, partition, offset);
			writeToFile(message, topic, partition, offset);
		}
	}

	@KafkaListener(topics = "hermes-message-copy",
			containerFactory = "messageCopyKafkaListenerContainerFactory")
	public void listenMessageCopy(@Payload String message,
			@Header(KafkaHeaders.RECEIVED_TOPIC) String topic,
			@Header(KafkaHeaders.RECEIVED_PARTITION_ID) int partition,
			@Header(KafkaHeaders.OFFSET) int offset)
	{
		log.info("Received message from topic={}, partition={}, offset={}",
				topic, partition, offset);
		if (message.contains(inputProperties.getSearchPatternInMessage())) {
			log.info(
					"Processing message from topic={}, partition={}, offset={}",
					topic, partition, offset);
			writeToFile(message, topic, partition, offset);
		}
	}

	@KafkaListener(topics = "hermes-request-log",
			containerFactory = "requestLogKafkaListenerContainerFactory")
	public void listenRequestLog(@Payload String message,
			@Header(KafkaHeaders.RECEIVED_TOPIC) String topic,
			@Header(KafkaHeaders.RECEIVED_PARTITION_ID) int partition,
			@Header(KafkaHeaders.OFFSET) int offset)
	{
		log.info("Received message from topic={}, partition={}, offset={}",
				topic, partition, offset);
		if (message.contains(inputProperties.getSearchPatternInMessage())) {
			log.info(
					"Processing message from topic={}, partition={}, offset={}",
					topic, partition, offset);
			writeToFile(message, topic, partition, offset);
		}
	}

	@KafkaListener(topics = "hermes-email-log",
			containerFactory = "emailLogKafkaListenerContainerFactory")
	public void listenEmailLog(@Payload String message,
			@Header(KafkaHeaders.RECEIVED_TOPIC) String topic,
			@Header(KafkaHeaders.RECEIVED_PARTITION_ID) int partition,
			@Header(KafkaHeaders.OFFSET) int offset)
	{
		log.info("Received message from topic={}, partition={}, offset={}",
				topic, partition, offset);
		if (message.contains(inputProperties.getSearchPatternInMessage())) {
			log.info(
					"Processing message from topic={}, partition={}, offset={}",
					topic, partition, offset);
			writeToFile(message, topic, partition, offset);
		}
	}

	@KafkaListener(topics = "hermes-sms-log",
			containerFactory = "smsLogKafkaListenerContainerFactory")
	public void listenSmsLog(@Payload String message,
			@Header(KafkaHeaders.RECEIVED_TOPIC) String topic,
			@Header(KafkaHeaders.RECEIVED_PARTITION_ID) int partition,
			@Header(KafkaHeaders.OFFSET) int offset)
	{
		log.info("Received message from topic={}, partition={}, offset={}",
				topic, partition, offset);
		if (message.contains(inputProperties.getSearchPatternInMessage())) {
			log.info(
					"Processing message from topic={}, partition={}, offset={}",
					topic, partition, offset);
			writeToFile(message, topic, partition, offset);
		}
	}

	@KafkaListener(topics = "hermes-push-log",
			containerFactory = "pushLogKafkaListenerContainerFactory")
	public void listenPushLog(@Payload String message,
			@Header(KafkaHeaders.RECEIVED_TOPIC) String topic,
			@Header(KafkaHeaders.RECEIVED_PARTITION_ID) int partition,
			@Header(KafkaHeaders.OFFSET) int offset)
	{
		log.info("Received message from topic={}, partition={}, offset={}",
				topic, partition, offset);
		if (message.contains(inputProperties.getSearchPatternInMessage())) {
			log.info(
					"Processing message from topic={}, partition={}, offset={}",
					topic, partition, offset);
			writeToFile(message, topic, partition, offset);
		}
	}

	private void writeToFile(String message, String topic, int partition,
			int offset)
	{
		String outputFilePath =
				outputProperties.getFolderPath() + "/" + topic + "/" + partition
						+ "-" + offset + ".json";
		try (FileWriter fileWriter = new FileWriter(outputFilePath);
				BufferedWriter bufferedWriter = new BufferedWriter(
						fileWriter)) {
			bufferedWriter.write(message);
		} catch (IOException e) {
			log.error(
					"Exception while writing in file for partition {} and offset {}",
					partition, offset, e);
		}
	}
}
