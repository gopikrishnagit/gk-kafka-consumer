package ind.gopi.codelabs.kafka.consumer.service;

import ind.gopi.codelabs.kafka.consumer.config.InputProperties;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.annotation.PartitionOffset;
import org.springframework.kafka.annotation.TopicPartition;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Service;

@Slf4j
@Service
public class KafkaConsumer
{
	@Autowired
	private InputProperties inputProperties;

	@Autowired
	private FileWriterService fileWriterService;

	@Autowired
	private KakfaProducerService kakfaProducerService;

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
			if (inputProperties.isWriteToFile()) {
				fileWriterService
						.writeToFile(message, topic, partition, offset);
			}
			if (inputProperties.isProducetoDestBrokers()) {
				kakfaProducerService.produce("hermes-error-log", message);
			}
		}
	}

	@KafkaListener(topicPartitions = @TopicPartition(topic = "hermes-message-log",
			partitionOffsets = { @PartitionOffset(partition = "0",
					initialOffset = "98690489") }),
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
			if (inputProperties.isWriteToFile()) {
				fileWriterService
						.writeToFile(message, topic, partition, offset);
			}
			if (inputProperties.isProducetoDestBrokers()) {
				kakfaProducerService.produce("hermes-message-log", message);
			}
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
			if (inputProperties.isWriteToFile()) {
				fileWriterService
						.writeToFile(message, topic, partition, offset);
			}
			if (inputProperties.isProducetoDestBrokers()) {
				kakfaProducerService.produce("hermes-message-copy", message);
			}
		}
	}

	@KafkaListener(topicPartitions = @TopicPartition(topic = "hermes-request-log",
			partitionOffsets = { @PartitionOffset(partition = "0",
					initialOffset = "67051349") }),
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
			if (inputProperties.isWriteToFile()) {
				fileWriterService
						.writeToFile(message, topic, partition, offset);
			}
			if (inputProperties.isProducetoDestBrokers()) {
				kakfaProducerService.produce("hermes-request-log", message);
			}
		}
	}

	@KafkaListener(topicPartitions = @TopicPartition(topic = "hermes-email-log",
			partitionOffsets = { @PartitionOffset(partition = "0",
					initialOffset = "13341877") }),
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
			if (inputProperties.isWriteToFile()) {
				fileWriterService
						.writeToFile(message, topic, partition, offset);
			}
			if (inputProperties.isProducetoDestBrokers()) {
				kakfaProducerService.produce("hermes-email-log", message);
			}
		}
	}

	@KafkaListener(topicPartitions = @TopicPartition(topic = "hermes-sms-log",
			partitionOffsets = { @PartitionOffset(partition = "0",
					initialOffset = "48048951") }),
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
			if (inputProperties.isWriteToFile()) {
				fileWriterService
						.writeToFile(message, topic, partition, offset);
			}
			if (inputProperties.isProducetoDestBrokers()) {
				kakfaProducerService.produce("hermes-sms-log", message);
			}
		}
	}

	@KafkaListener(topicPartitions = @TopicPartition(topic = "hermes-push-log",
			partitionOffsets = { @PartitionOffset(partition = "0",
					initialOffset = "22739207") }),
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
			if (inputProperties.isWriteToFile()) {
				fileWriterService
						.writeToFile(message, topic, partition, offset);
			}
			if (inputProperties.isProducetoDestBrokers()) {
				kakfaProducerService.produce("hermes-push-log", message);
			}
		}
	}
}
