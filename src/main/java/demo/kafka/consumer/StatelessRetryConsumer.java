package demo.kafka.consumer;

import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;

import demo.kafka.exception.Retryable;
import demo.kafka.service.DemoRetryService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.messaging.Message;
import org.springframework.stereotype.Component;

/**
 * A listener that demonstrates stateless retry.
 *
 * The retry happens within a poll, so is at risk of exceeding the poll timeout.
 */
@Slf4j
@RequiredArgsConstructor
@Component
public class StatelessRetryConsumer {

    final AtomicInteger counter = new AtomicInteger();
    final DemoRetryService demoRetryService;

    @KafkaListener(topics = "demo-stateless-retry-topic", groupId = "kafkaStatelessRetryConsumerGroup", containerFactory = "kafkaStatelessRetryListenerContainerFactory")
    public void listen(final Message message) {
        counter.getAndIncrement();
        log.debug("Received message [" +counter.get()+ "]: " + message.getPayload());
        String id = message.getPayload().toString();
        try {
            demoRetryService.process(id);
        } catch (Exception e) {
            // a retryable exception is thrown, any thing else is logged and the message is marked as consumed
            if (e instanceof Retryable) {
                throw e;
            }
            log.error("Error processing message: " + e.getMessage());
        }
    }

    public AtomicInteger getCounter() {
        return counter;
    }
}
