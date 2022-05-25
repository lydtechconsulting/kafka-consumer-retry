package demo.kafka.integration;

import java.util.concurrent.TimeUnit;

import demo.kafka.consumer.StatelessRetryConsumer;
import lombok.extern.slf4j.Slf4j;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.test.context.TestPropertySource;

import static com.github.tomakehurst.wiremock.client.WireMock.exactly;
import static com.github.tomakehurst.wiremock.client.WireMock.getRequestedFor;
import static com.github.tomakehurst.wiremock.client.WireMock.urlEqualTo;
import static com.github.tomakehurst.wiremock.client.WireMock.verify;
import static org.hamcrest.Matchers.equalTo;

@Slf4j
@EmbeddedKafka(controlledShutdown = true, topics = "demo-stateless-retry-topic")
@TestPropertySource(properties="kafka.consumer.maxPollIntervalMs=30000")
public class KafkaStatelessRetryPollIntervalNotExceededIntegrationTest extends IntegrationTestBase {

    private final static String STATELESS_RETRY_TEST_TOPIC = "demo-stateless-retry-topic";

    @Autowired
    private StatelessRetryConsumer consumer;

    /**
     * The poll timeout is set to 30 seconds (via @TestPropertySource).
     *
     * The retry is configured to retry 4 times with a 4 second pause.
     *
     * As the retry is stateless it happens within the poll.  So the poll will not time out.
     *
     * The message is re-delivered the configured 4 times before completion.
     */
    @Test
    public void testStatelessRetry_PollIntervalNotExceeded() throws Exception {
        // prime the rest api mock to return service unavailable every time it's called
        stubWiremock("/api/kafkaretrydemo/" + requestId, 500, "Unavailable");

        sendMessage(STATELESS_RETRY_TEST_TOPIC, requestId);

        Awaitility.await().atMost(30, TimeUnit.SECONDS).pollDelay(20, TimeUnit.SECONDS)
                .until(consumer.getCounter()::get, equalTo(5));
        verify(exactly(5), getRequestedFor(urlEqualTo("/api/kafkaretrydemo/" + requestId)));
    }
}
