package ind.gopi.codelabs.kafka.consumer;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.EnableConfigurationProperties;

@SpringBootApplication
@EnableConfigurationProperties
public class GkKafkaConsumerApplication {

	public static void main(String[] args) {
		SpringApplication.run(GkKafkaConsumerApplication.class, args);
	}

}
