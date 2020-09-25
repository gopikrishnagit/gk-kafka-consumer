package ind.gopi.codelabs.kafka.consumer.config;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.listener.ContainerProperties;

import java.util.HashMap;
import java.util.Map;

@EnableKafka
@Configuration
public class KafkaConsumerConfig
{
	public static final String ERROR_LOG_CONSUMER_GROUP = "gk-localconsumer-errorlog-1";
	public static final String MESSAGE_LOG_CONSUMER_GROUP = "gk-localconsumer-messagelog-1";
	public static final String MESSAGE_COPY_CONSUMER_GROUP = "gk-localconsumer-messagecopy-1";
	public static final String REQUEST_LOG_CONSUMER_GROUP = "gk-localconsumer-requestlog-1";
	public static final String EMAIL_LOG_CONSUMER_GROUP = "gk-localconsumer-emaillog-1";
	public static final String SMS_LOG_CONSUMER_GROUP = "gk-localconsumer-smslog-1";
	public static final String PUSH_LOG_CONSUMER_GROUP = "gk-localconsumer-pushlog-1";

	@Bean
	public ConcurrentKafkaListenerContainerFactory<String, String> errorLogKafkaListenerContainerFactory(
			KafkaProperties kafkaProperties)
	{
		ConcurrentKafkaListenerContainerFactory<String, String> factory = new ConcurrentKafkaListenerContainerFactory<>();
		factory.setConsumerFactory(
				getConsumerFactory(kafkaProperties, ERROR_LOG_CONSUMER_GROUP));
		factory.setConcurrency(kafkaProperties.getListenerConcurrency());
		factory.getContainerProperties()
				.setAckMode(ContainerProperties.AckMode.RECORD);
		return factory;
	}

	@Bean
	public ConcurrentKafkaListenerContainerFactory<String, String> messageLogKafkaListenerContainerFactory(
			KafkaProperties kafkaProperties)
	{
		ConcurrentKafkaListenerContainerFactory<String, String> factory = new ConcurrentKafkaListenerContainerFactory<>();
		factory.setConsumerFactory(getConsumerFactory(kafkaProperties,
				MESSAGE_LOG_CONSUMER_GROUP));
		factory.setConcurrency(kafkaProperties.getListenerConcurrency());
		factory.getContainerProperties()
				.setAckMode(ContainerProperties.AckMode.RECORD);
		return factory;
	}

	@Bean
	public ConcurrentKafkaListenerContainerFactory<String, String> messageCopyKafkaListenerContainerFactory(
			KafkaProperties kafkaProperties)
	{
		ConcurrentKafkaListenerContainerFactory<String, String> factory = new ConcurrentKafkaListenerContainerFactory<>();
		factory.setConsumerFactory(getConsumerFactory(kafkaProperties,
				MESSAGE_COPY_CONSUMER_GROUP));
		factory.setConcurrency(kafkaProperties.getListenerConcurrency());
		factory.getContainerProperties()
				.setAckMode(ContainerProperties.AckMode.RECORD);
		return factory;
	}

	@Bean
	public ConcurrentKafkaListenerContainerFactory<String, String> requestLogKafkaListenerContainerFactory(
			KafkaProperties kafkaProperties)
	{
		ConcurrentKafkaListenerContainerFactory<String, String> factory = new ConcurrentKafkaListenerContainerFactory<>();
		factory.setConsumerFactory(getConsumerFactory(kafkaProperties,
				REQUEST_LOG_CONSUMER_GROUP));
		factory.setConcurrency(kafkaProperties.getListenerConcurrency());
		factory.getContainerProperties()
				.setAckMode(ContainerProperties.AckMode.RECORD);
		return factory;
	}

	@Bean
	public ConcurrentKafkaListenerContainerFactory<String, String> emailLogKafkaListenerContainerFactory(
			KafkaProperties kafkaProperties)
	{
		ConcurrentKafkaListenerContainerFactory<String, String> factory = new ConcurrentKafkaListenerContainerFactory<>();
		factory.setConsumerFactory(
				getConsumerFactory(kafkaProperties, EMAIL_LOG_CONSUMER_GROUP));
		factory.setConcurrency(kafkaProperties.getListenerConcurrency());
		factory.getContainerProperties()
				.setAckMode(ContainerProperties.AckMode.RECORD);
		return factory;
	}

	@Bean
	public ConcurrentKafkaListenerContainerFactory<String, String> smsLogKafkaListenerContainerFactory(
			KafkaProperties kafkaProperties)
	{
		ConcurrentKafkaListenerContainerFactory<String, String> factory = new ConcurrentKafkaListenerContainerFactory<>();
		factory.setConsumerFactory(
				getConsumerFactory(kafkaProperties, SMS_LOG_CONSUMER_GROUP));
		factory.setConcurrency(kafkaProperties.getListenerConcurrency());
		factory.getContainerProperties()
				.setAckMode(ContainerProperties.AckMode.RECORD);
		return factory;
	}

	@Bean
	public ConcurrentKafkaListenerContainerFactory<String, String> pushLogKafkaListenerContainerFactory(
			KafkaProperties kafkaProperties)
	{
		ConcurrentKafkaListenerContainerFactory<String, String> factory = new ConcurrentKafkaListenerContainerFactory<>();
		factory.setConsumerFactory(
				getConsumerFactory(kafkaProperties, PUSH_LOG_CONSUMER_GROUP));
		factory.setConcurrency(kafkaProperties.getListenerConcurrency());
		factory.getContainerProperties()
				.setAckMode(ContainerProperties.AckMode.RECORD);
		return factory;
	}

	private ConsumerFactory<String, String> getConsumerFactory(
			KafkaProperties properties, String consumerGroup)
	{
		Map<String, Object> props = new HashMap<>();
		props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,
				properties.getBootstrapServers());
		props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
		props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG,
				properties.getMaxPollRecords());
		props.put(ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG,
				properties.getMaxPollIntervalMs());
		props.put(ConsumerConfig.HEARTBEAT_INTERVAL_MS_CONFIG,
				properties.getHeartbeatIntervalMs());
		props.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG,
				properties.getSessionTimeoutMs());
		props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
				StringDeserializer.class);
		props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
				StringDeserializer.class);
		props.put(ConsumerConfig.GROUP_ID_CONFIG, consumerGroup);

		return new DefaultKafkaConsumerFactory<>(props);
	}
}