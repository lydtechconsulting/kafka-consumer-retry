package demo.kafka.consumer;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import demo.kafka.exception.Retryable;
import demo.kafka.service.DemoRetryService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.messaging.Message;
import org.springframework.stereotype.Component;

/**
 * A listener that demonstrates standard retry scenarios.
 */
@Slf4j
@RequiredArgsConstructor
@Component
public class StandardRetryConsumer {

    final AtomicInteger counter = new AtomicInteger();
    final DemoRetryService demoRetryService;
    final Map<String,String> sentState = new HashMap<>();

    @KafkaListener(topics = "demo-standard-retry-topic", groupId = "kafkaStandardRetryConsumerGroup", containerFactory = "kafkaStatefulRetryListenerContainerFactory")
    public void listen(final Message message) {
        log.debug("Received message [" +counter.get()+ "]: " + message.getPayload());
        counter.getAndIncrement();
        String payload = message.getPayload().toString();
        String requestId = demoRetryService.extractRequestIdFromEventPayload(payload);
        try {
            demoRetryService.process(payload);
            sentState.put(requestId, "SENT");
            log.debug("Successfully processed message [" +counter.get()+ "]: " + message.getPayload());
        } catch (Exception e) {
            // a retryable exception is thrown, any thing else is logged and the message is marked as consumed
            if (e instanceof Retryable) {
                sentState.put(requestId, "RETRYING");
                throw e;
            }
            log.error("Error processing message: " + e.getMessage());
            sentState.put(requestId, "FAILED");
        }
    }

    public AtomicInteger getCounter() {
        return counter;
    }

    public String getSentState(String id) {
        return sentState.get(id);
    }

}
