package demo.kafka.integration;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import demo.kafka.KafkaDemoConfiguration;
import demo.kafka.consumer.StandardRetryConsumer;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.messaging.MessageHeaders;
import org.springframework.messaging.handler.annotation.Headers;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.test.annotation.DirtiesContext;

import static com.github.tomakehurst.wiremock.client.WireMock.exactly;
import static com.github.tomakehurst.wiremock.client.WireMock.getRequestedFor;
import static com.github.tomakehurst.wiremock.client.WireMock.urlEqualTo;
import static com.github.tomakehurst.wiremock.client.WireMock.verify;
import static com.github.tomakehurst.wiremock.stubbing.Scenario.STARTED;
import static org.hamcrest.Matchers.equalTo;

@Slf4j
@SpringBootTest(classes = { KafkaDemoConfiguration.class } )
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_EACH_TEST_METHOD)
@EmbeddedKafka(controlledShutdown = true, topics = "demo-standard-retry-topic")
public class KafkaStandardRetryIntegrationTest extends IntegrationTestBase {

    final static String RETRY_TEST_TOPIC = "demo-standard-retry-topic";

    @Autowired
    private StandardRetryConsumer consumer;

    @Autowired
    private KafkaTestListener testReceiver;

    @Configuration
    static class TestConfig {

        @Bean
        public KafkaTestListener testReceiver() {
            return new KafkaTestListener();
        }

        @Bean
        public ConsumerFactory<Object, Object> testConsumerFactory(@Value("${kafka.bootstrap-servers}") final String bootstrapServers) {
            final Map<String, Object> config = new HashMap<>();
            config.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
            config.put(ConsumerConfig.GROUP_ID_CONFIG, "KafkaStandardRetryIntegrationTest");
            config.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
            config.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
            return new DefaultKafkaConsumerFactory<>(config);
        }
    }

    /**
     * Use this receiver to consume messages from the outbound topic.
     */
    public static class KafkaTestListener {
        CountDownLatch latch = new CountDownLatch(1);

        @KafkaListener(groupId = "KafkaStandardRetryIntegrationTest", topics = "demo-outbound-topic", autoStartup = "true")
        void receive(@Payload final String payload, @Headers final MessageHeaders headers) {
            log.debug("KafkaTestListener - Received message: " + payload);
            latch.countDown();
        }
    }

    @Test
    public void testSuccess() throws Exception {
        // prime the rest api mock to return a success response
        stubWiremock("/api/kafkaretrydemo/" + requestId, 200, "Success");

        sendMessage(RETRY_TEST_TOPIC, requestId);

        Awaitility.await().atMost(5, TimeUnit.SECONDS).pollDelay(1, TimeUnit.SECONDS)
                .until(()-> consumer.getSentState(requestId).equals("SENT"));
        verify(exactly(1), getRequestedFor(urlEqualTo("/api/kafkaretrydemo/" + requestId)));

        // check for a message being emitted on demo-standard-outbound-topic
        testReceiver.latch.await(10, TimeUnit.SECONDS);
        assert testReceiver.latch.getCount() == 0;
    }

    @Test
    public void testNonRetryableError() throws Exception {
        // prime the rest api mock to return a non-retriable error
        stubWiremock("/api/kafkaretrydemo/" + requestId, 400, "Bad request");

        sendMessage(RETRY_TEST_TOPIC, requestId);

        Awaitility.await().atMost(5, TimeUnit.SECONDS).pollDelay(3, TimeUnit.SECONDS)
                .until(consumer.getCounter()::get, equalTo(1));
        Awaitility.await().atMost(5, TimeUnit.SECONDS).pollDelay(1, TimeUnit.SECONDS)
                .until(()-> consumer.getSentState(requestId).equals("FAILED"));
        verify(exactly(1), getRequestedFor(urlEqualTo("/api/kafkaretrydemo/" + requestId)));
    }

    @Test
    public void testInitialFailureThenSuccess() throws Exception {
        // prime the rest api mock to return service unavailable on the first call then return a success
        stubWiremock("/api/kafkaretrydemo/" + requestId, 500, "Unavailable", "failOnce", STARTED, "succeedNextTime");
        stubWiremock("/api/kafkaretrydemo/" + requestId, 200, "success", "failOnce", "succeedNextTime", "succeedNextTime");

        sendMessage(RETRY_TEST_TOPIC, requestId);

        Awaitility.await().atMost(5, TimeUnit.SECONDS).pollDelay(1, TimeUnit.SECONDS)
                .until(()-> consumer.getSentState(requestId).equals("SENT"));
        Awaitility.await().atMost(5, TimeUnit.SECONDS).pollDelay(3, TimeUnit.SECONDS)
                .until(consumer.getCounter()::get, equalTo(2));
        verify(exactly(2), getRequestedFor(urlEqualTo("/api/kafkaretrydemo/" + requestId)));

        // check for a message being emitted on demo-standard-outbound-topic
        testReceiver.latch.await(10, TimeUnit.SECONDS);
        assert testReceiver.latch.getCount() == 0;
    }

    @Test
    public void testRetryUntilFail() throws Exception {
        // prime the rest api mock to return service unavailable every time it's called
        stubWiremock("/api/kafkaretrydemo/" + requestId, 500, "Unavailable");

        sendMessage(RETRY_TEST_TOPIC, requestId);

        Awaitility.await().atMost(20, TimeUnit.SECONDS).pollDelay(3, TimeUnit.SECONDS)
                .until(consumer.getCounter()::get, equalTo(4));
        verify(exactly(4), getRequestedFor(urlEqualTo("/api/kafkaretrydemo/" + requestId)));
    }
}
