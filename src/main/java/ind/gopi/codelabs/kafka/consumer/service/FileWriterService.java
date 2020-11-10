package ind.gopi.codelabs.kafka.consumer.service;

import ind.gopi.codelabs.kafka.consumer.config.OutputProperties;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
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

	public void writeToFile(String message, String topic, int partition,
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
