package gr.edu.thesis.inquiry.controller;

import org.apache.camel.ProducerTemplate;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/api/v1")
public class InquiryController {

    @Autowired
    private ProducerTemplate producerTemplate;

    @GetMapping("/balance/{accountId}")
    public ResponseEntity<String> getBalance(@PathVariable String accountId) {
        // Στέλνουμε το accountId στον Camel Route
        producerTemplate.sendBody("direct:sendInquiry", accountId);
        
        return ResponseEntity.ok("Request for " + accountId + " submitted to queue.");
    }
}