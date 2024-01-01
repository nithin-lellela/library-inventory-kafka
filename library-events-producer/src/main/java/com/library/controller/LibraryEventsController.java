package com.library.controller;

import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.library.domain.LibraryEvent;
import com.library.domain.LibraryEventType;
import com.library.producer.LibraryEventsProducer;

import jakarta.validation.Valid;
import lombok.extern.slf4j.Slf4j;

@RestController
@Slf4j
public class LibraryEventsController {
	
	private final LibraryEventsProducer libraryEventsProducer;
	
	public LibraryEventsController(LibraryEventsProducer libraryEventsProducer) {
		this.libraryEventsProducer = libraryEventsProducer;
	}

	@PostMapping("/v1/libraryevent")
	public ResponseEntity<LibraryEvent> postLibraryEvent(
			@RequestBody LibraryEvent libraryEvent) throws JsonProcessingException{
		
		log.info("libraryEvent: {}", libraryEvent);
		libraryEventsProducer.sendLibraryEvent(libraryEvent);
		log.info("Library Event completion log");
		return ResponseEntity.status(HttpStatus.CREATED).body(libraryEvent);
	
	}
	
	@PutMapping("/v1/libraryevent")
	public ResponseEntity<?> updateLibraryEvent(
			@RequestBody @Valid LibraryEvent libraryEvent) throws JsonProcessingException{
		
		log.info("libraryEvent: {}", libraryEvent);
		ResponseEntity<String> BAD_REQUEST = validateLibraryEvent(libraryEvent);
		if(BAD_REQUEST != null) {
			return BAD_REQUEST;
		}
		
		libraryEventsProducer.sendLibraryEvent(libraryEvent);
		return ResponseEntity.status(HttpStatus.CREATED).body(libraryEvent);
	
	}

	private ResponseEntity<String> validateLibraryEvent(LibraryEvent libraryEvent) {
		if(libraryEvent.libraryEventId() == null) {
			return ResponseEntity.status(HttpStatus.BAD_REQUEST).body("Please pass the library event ID");
		}
		if(libraryEvent.libraryEventType().equals(LibraryEventType.UPDATE)) {
			return ResponseEntity.status(HttpStatus.BAD_REQUEST).body("Only UPDATE event type is supported");
		}
		return null;
	}
	
}
