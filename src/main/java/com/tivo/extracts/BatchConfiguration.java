package com.tivo.extracts;

import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.launch.support.RunIdIncrementer;
import org.springframework.batch.item.file.FlatFileItemWriter;
import org.springframework.batch.item.file.builder.FlatFileItemWriterBuilder;
import org.springframework.batch.item.file.transform.DelimitedLineTokenizer;
import org.springframework.batch.item.json.JacksonJsonObjectMarshaller;
import org.springframework.batch.item.json.JacksonJsonObjectReader;
import org.springframework.batch.item.json.JsonFileItemWriter;
import org.springframework.batch.item.json.JsonItemReader;
import org.springframework.batch.item.json.builder.JsonFileItemWriterBuilder;
import org.springframework.batch.item.json.builder.JsonItemReaderBuilder;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.ClassPathResource;
import org.springframework.core.io.FileSystemResource;
import org.springframework.core.io.Resource;

import com.fasterxml.jackson.databind.ObjectMapper;

@Configuration
@EnableBatchProcessing
public class BatchConfiguration {

	@Autowired
	public JobBuilderFactory jobBuilderFactory;

	@Autowired
	public StepBuilderFactory stepBuilderFactory;

	private Resource outputResource = new FileSystemResource("/output/trade.tsv");
	
	private Resource JSONOutputResource = new FileSystemResource("/output/tradesWriter.json");

	@Bean
	public JsonItemReader<Trade> jsonItemReader() {

		ObjectMapper objectMapper = new ObjectMapper();
		// configure the objectMapper as required
		JacksonJsonObjectReader<Trade> jsonObjectReader = new JacksonJsonObjectReader<>(Trade.class);
		jsonObjectReader.setMapper(objectMapper);

		return new JsonItemReaderBuilder<Trade>().jsonObjectReader(jsonObjectReader)
				.resource(new ClassPathResource("trades.json")).name("tradeJsonItemReader").build();
	}

	@Bean
	public FlatFileItemWriter<Trade> writer() {
		// Create a writer instance
		FlatFileItemWriter<Trade> writer = new FlatFileItemWriter<>();

		// set file location for wrting the trade details.
		writer.setResource(outputResource);

		// Flag to indicate if the file should be appended if already exist.
		writer.setAppendAllowed(true);

		return new FlatFileItemWriterBuilder<Trade>().name("tradeFileItemWriter").resource(outputResource).delimited()
				.delimiter(DelimitedLineTokenizer.DELIMITER_TAB)
				.names(new String[] { "isin", "quantity", "price", "customer" }).build();
	}
	
	@Bean
	public JsonFileItemWriter<Trade> jsonFileItemWriter() {
	   return new JsonFileItemWriterBuilder<Trade>()
	                 .jsonObjectMarshaller(new JacksonJsonObjectMarshaller<>())
	                 .resource(JSONOutputResource)
	                 .name("tradeJsonFileItemWriter")
	                 .build();
	}

	@Bean
	public Job tradeJob(JobCompletionNotificationListener listener, Step step1) {
		return jobBuilderFactory.get("tradeJob").incrementer(new RunIdIncrementer()).listener(listener).flow(step1)
				.end().build();

	}

	@Bean
	public Step step1() {
		return stepBuilderFactory.get("step1").<Trade, Trade>chunk(10).reader(jsonItemReader()).writer(jsonFileItemWriter())
				.build();
	}
}
