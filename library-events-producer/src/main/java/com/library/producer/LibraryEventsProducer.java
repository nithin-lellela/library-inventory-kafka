package com.library.producer;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

import org.apache.kafka.clients.producer.ProducerRecord;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Component;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.library.domain.LibraryEvent;

import lombok.extern.slf4j.Slf4j;

@Component
@Slf4j
public class LibraryEventsProducer {
	
	@Value("${spring.kafka.topic}")
	public String topic;
	
	private final KafkaTemplate<Integer, String> kafkaTemplate;
	
	private final ObjectMapper objectMapper;
	
	public LibraryEventsProducer(KafkaTemplate<Integer, String> kafkaTemplate,
			ObjectMapper objectMapper) {
		this.kafkaTemplate = kafkaTemplate;
		this.objectMapper = objectMapper;
	}
	
	public CompletableFuture<SendResult<Integer, String>> sendLibraryEvent(LibraryEvent libraryEvent) 
			throws JsonProcessingException {
		
		Integer key = libraryEvent.libraryEventId();
		String value = objectMapper.writeValueAsString(libraryEvent);
		
		// 1.blocking call (only for the very first request) - gets metadata about the kafka cluster 
		//(i.e see console logs for reference) 
		// if the first call to kafka cluster fails, it runs into a failure scenario. we won't be able to send any data
		// 2.send message happens asynchronously - returns a CompletableFuture
		CompletableFuture<SendResult<Integer, String>> completableFuture =  
				kafkaTemplate.send(topic, key, value);
		return completableFuture.whenComplete((sendResult, throwable) -> {
			if(throwable != null) {
				handleFailure(key, value, throwable);
			}else {
				handleSuccess(key, value, sendResult);
			}
		});
		
	}
	
	public SendResult<Integer, String> sendLibraryEvent2(LibraryEvent libraryEvent) 
			throws InterruptedException, ExecutionException, JsonProcessingException{
		Integer key = libraryEvent.libraryEventId();
		String value = objectMapper.writeValueAsString(libraryEvent);
		
		// Synchronous way, Block and wait until the message is sent to kafka for every request
		SendResult<Integer, String> result = kafkaTemplate.send(topic, key, value).get();
		/* 
		 * Block and wait till 3 sec for the message is sent to kafka for every request,
		 * if we won't get anything till 3 secs, this going to timeout and throws Exception
		  SendResult<Integer, String> result = kafkaTemplate
		 
				.send(topic, key, value)
				.get(3, TimeUnit.SECONDS); 
		*/
		handleSuccess(key, value, result);
		return result;
	}
	
	public CompletableFuture<SendResult<Integer, String>> sendLibraryEvent3(LibraryEvent libraryEvent) 
			throws JsonProcessingException {
		
		Integer key = libraryEvent.libraryEventId();
		String value = objectMapper.writeValueAsString(libraryEvent);
		ProducerRecord<Integer, String> record = buildProducerRecord(key, value);
		
		// 1.blocking call (only for the very first request) - gets metadata about the kafka cluster 
		//(i.e see console logs for reference) 
		// if the first call to kafka cluster fails, it runs into a failure scenario. we won't be able to send any data
		// 2.send message happens asynchronously - returns a CompletableFuture
		CompletableFuture<SendResult<Integer, String>> completableFuture =  
				kafkaTemplate.send(record);
		return completableFuture.whenComplete((sendResult, throwable) -> {
			if(throwable != null) {
				handleFailure(key, value, throwable);
			}else {
				handleSuccess(key, value, sendResult);
			}
		});
		
	}

	private ProducerRecord<Integer, String> buildProducerRecord(Integer key, String value) {
		return new ProducerRecord<>(topic, key, value);
	}

	private void handleSuccess(Integer key, String value, SendResult<Integer, String> sendResult) {
		
		log.info("Message is send successfully for the key: {} and the value: {}, partition is {}",
				key, value, sendResult.getRecordMetadata().partition());
		
	}

	private void handleFailure(Integer key, String value, Throwable throwable) {
		
		log.error("Error in sending the Message and the execption is {}", throwable.getMessage() ,throwable);
		
	}
	
}
