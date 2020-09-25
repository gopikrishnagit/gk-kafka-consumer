package ind.gopi.codelabs.kafka.consumer.config;

import lombok.Getter;
import lombok.Setter;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Configuration;

@Configuration
@ConfigurationProperties(prefix = "input")
@Getter
@Setter
public class InputProperties
{
	private String searchPatternInMessage;
}
