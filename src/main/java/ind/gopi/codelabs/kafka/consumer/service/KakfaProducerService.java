package ind.gopi.codelabs.kafka.consumer.service;

import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Service;
import org.springframework.util.concurrent.ListenableFuture;
import org.springframework.util.concurrent.ListenableFutureCallback;

@Slf4j
@Service
public class KakfaProducerService
{
	@Autowired
	private KafkaTemplate<String, String> kafkaTemplate;

	public void produce(String topicName, String data)
	{
		log.info("Producing into topic: " + topicName);
		ListenableFuture<SendResult<String, String>> future = kafkaTemplate
				.send(topicName, data);

		future.addCallback(
				new ListenableFutureCallback<SendResult<String, String>>()
				{

					@Override
					public void onSuccess(SendResult<String, String> result)
					{
						log.info("Sent message with offset=[" + result
								.getRecordMetadata().offset() + "]");
					}

					@Override
					public void onFailure(Throwable ex)
					{
						log.error("Unable to send message due to : " + ex
								.getMessage());
					}
				});
	}
}
