package gr.edu.thesis.inquiry.controller;

import org.apache.camel.ProducerTemplate;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

// Include Micrometer imports for Prometheus integration
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;

@RestController
@RequestMapping("/api/v1")
public class InquiryController {

    private final ProducerTemplate producerTemplate;
    
    // Setting up a Counter to track the number of requests received at this endpoint
    private final Counter requestCounter;

    // Using constructor injection to get the ProducerTemplate and MeterRegistry from Spring's context
    @Autowired
    public InquiryController(ProducerTemplate producerTemplate, MeterRegistry registry) {
        this.producerTemplate = producerTemplate;
        
        // Creating and registering a Counter metric with Micrometer to track the total number of requests
        this.requestCounter = Counter.builder("inquiry_requests_total")
                .description("Total banking inquiry requests received by the backend")
                .tag("endpoint", "balance") 
                .register(registry);
    }

    @GetMapping("/balance/{accountId}")
    public ResponseEntity<String> getBalance(@PathVariable String accountId) {
        
        // Accessing the Counter metric and incrementing it by 1 for each request received at this endpoint
        requestCounter.increment(); 

        // Sending the accountId to the Camel route "direct:sendInquiry" which will handle the processing of the inquiry
        producerTemplate.sendBody("direct:sendInquiry", accountId);
        
        return ResponseEntity.ok("Request for " + accountId + " submitted to queue.");
    }
}