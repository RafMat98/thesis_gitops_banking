package gr.edu.thesis.inquiry.route;

import org.apache.camel.builder.RouteBuilder;
import org.springframework.stereotype.Component;

@Component
public class InquiryRoute extends RouteBuilder {

    @Override
    public void configure() throws Exception {
        
        from("direct:sendInquiry")
            .routeId("KafkaProducerRoute")
            .log("Sending inquiry for account: ${body}")
            .to("kafka:balance-requests")
            .log("Message sent to Kafka successfully");
    }
}