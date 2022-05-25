package demo.kafka.component;

import java.time.Duration;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import dev.lydtech.component.framework.client.kafka.KafkaClient;
import dev.lydtech.component.framework.client.wiremock.WiremockClient;
import dev.lydtech.component.framework.extension.TestContainersSetupExtension;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

@Slf4j
@ExtendWith(TestContainersSetupExtension.class)
public class RetryCT {

    private static final String GROUP_ID = "RetryComponentTest";

    private Consumer consumer;

    /**
     * Configure the wiremock to return a 503 three times before success.
     */
    @BeforeEach
    public void setup() {
        consumer = KafkaClient.getInstance().createConsumer(GROUP_ID, "demo-outbound-topic");

        WiremockClient.getInstance().resetMappings();
        WiremockClient.getInstance().postMappingFile("thirdParty/retry_behaviour_01_start-to-unavailable.json");
        WiremockClient.getInstance().postMappingFile("thirdParty/retry_behaviour_02_unavailable-to-unavailable2.json");
        WiremockClient.getInstance().postMappingFile("thirdParty/retry_behaviour_03_unavailable2-to-success.json");
        WiremockClient.getInstance().postMappingFile("thirdParty/retry_behaviour_04_success.json");

        // Clear the topic.
        consumer.poll(Duration.ofSeconds(1));
    }

    @AfterEach
    public void tearDown() {
        consumer.close();
    }

    /**
     * Stateless retry means the retries will exceed the poll timeout, so the message will be re-delivered.
     */
    @Test
    public void testStatelessRetry() throws Exception {
        sendMessageAndAssert("Stateless", "demo-stateless-retry-topic", 2);
    }

    /**
     * Stateful retry means the retries will not exceed the poll timeout, so only one message will be delivered.
     */
    @Test
    public void testStatefulRetry() throws Exception {
        sendMessageAndAssert("Stateful", "demo-stateful-retry-topic", 1);
    }

    /**
     * 1. Send the message to the application's inbound topic.
     * 2. Poll for messages on the application's outbound topic.
     * 3. Assert the expected number are received.
     */
    public void sendMessageAndAssert(String retryType, String topic, int expectedEventCount) throws Exception {
        UUID key = UUID.randomUUID();
        UUID payload = UUID.randomUUID();
        KafkaClient.getInstance().sendMessage(topic, key.toString(), payload.toString());
        log.info(retryType + " retry - sent event: "+key);
        AtomicInteger totalReceivedEvents = new AtomicInteger();

        // Track the number of polls to ensure sufficient have happened to catch any duplicates.
        AtomicInteger pollCount = new AtomicInteger();

        Awaitility.await()
                .atMost(30, TimeUnit.SECONDS)
                .pollDelay(2, TimeUnit.SECONDS)
                .pollInterval(1, TimeUnit.SECONDS)
                .until(() -> {
                    final ConsumerRecords<String, String> consumerRecords = consumer.poll(Duration.ofMillis(500));
                    consumerRecords.forEach(record -> {
                        log.info(retryType + " retry - received: "+record.value());
                        totalReceivedEvents.incrementAndGet();
                    });
                    pollCount.getAndIncrement();
                    log.info(retryType + " retry ["+pollCount.get()+"]- test received count: "+totalReceivedEvents.get());
                    return pollCount.get() > 14 && totalReceivedEvents.get() == expectedEventCount;
                });
    }
}
