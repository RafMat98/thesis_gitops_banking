package gr.edu.thesis.inquiry.controller;

import org.apache.camel.ProducerTemplate;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Metrics; 

@RestController
@RequestMapping("/api/v1")
public class InquiryController {

    private final ProducerTemplate producerTemplate;
    private final Counter requestCounter;

    @Autowired
    public InquiryController(ProducerTemplate producerTemplate) { 
        this.producerTemplate = producerTemplate;
        
        this.requestCounter = Counter.builder("inquiry_requests_total")
                .description("Total banking inquiry requests received by the backend")
                .tag("endpoint", "balance")
                .register(Metrics.globalRegistry); 
    }

    @GetMapping("/balance/{accountId}")
    public ResponseEntity<String> getBalance(@PathVariable String accountId) {
        requestCounter.increment(); 
        producerTemplate.sendBody("direct:sendInquiry", accountId);
        return ResponseEntity.ok("Request for " + accountId + " submitted to queue.");
    }
}