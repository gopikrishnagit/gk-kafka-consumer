package ind.gopi.codelabs.kafka.consumer.config;

import lombok.Getter;
import lombok.Setter;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Configuration;

@Configuration
@ConfigurationProperties(prefix = "kafka")
@Getter
@Setter
public class KafkaProperties
{
	private String bootstrapServers;
	private boolean autoCommitConfig;
	private int maxPollRecords;
	private int maxPollIntervalMs;
	private int heartbeatIntervalMs;
	private int sessionTimeoutMs;
	private int listenerConcurrency;

}
