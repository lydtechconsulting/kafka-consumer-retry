package demo.kafka.integration;

import java.util.concurrent.TimeUnit;

import demo.kafka.consumer.StatelessRetryConsumer;
import lombok.extern.slf4j.Slf4j;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.test.context.TestPropertySource;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.greaterThan;

@Slf4j
@EmbeddedKafka(controlledShutdown = true, topics = "demo-stateless-retry-topic")
@TestPropertySource(properties="kafka.consumer.maxPollIntervalMs=10000")
public class KafkaStatelessRetryPollIntervalExceededIntegrationTest extends IntegrationTestBase {

    private final static String STATELESS_RETRY_TEST_TOPIC = "demo-stateless-retry-topic";

    @Autowired
    private StatelessRetryConsumer consumer;

    /**
     * The poll timeout is overridden to 10 seconds (via @TestPropertySource).
     *
     * The retry is configured to retry 4 times with a 4 second pause.
     *
     * As the retry is stateless it happens within the poll.  So the poll does time out.
     *
     * The consumer group re-balances.  Observe the logging which shows this.
     *
     * The message is re-delivered to the broker.
     *
     * Therefore we observe the message is continually re-delivered.
     */
    @Test
    public void testStatelessRetry_PollIntervalExceeded() throws Exception {
        // prime the rest api mock to return service unavailable every time it's called
        stubWiremock("/api/kafkaretrydemo/" + requestId, 500, "Unavailable");

        sendMessage(STATELESS_RETRY_TEST_TOPIC, requestId);

        Awaitility.await().atMost(30, TimeUnit.SECONDS).pollDelay(1, TimeUnit.SECONDS)
                .until(consumer.getCounter()::get, greaterThan(5));
        log.debug("First check: "+consumer.getCounter().get());

        // Prove that the message is being re-delivered.
        TimeUnit.SECONDS.sleep(10);
        log.debug("10 seconds on : "+consumer.getCounter().get());
        assertThat(consumer.getCounter().get(), greaterThan(6));
    }
}
