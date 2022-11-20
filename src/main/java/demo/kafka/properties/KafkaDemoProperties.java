package demo.kafka.properties;

import java.net.URL;
import java.util.UUID;

import lombok.Getter;
import lombok.Setter;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Configuration;
import org.springframework.lang.NonNull;
import org.springframework.validation.annotation.Validated;

@Configuration
@ConfigurationProperties("kafkademo")
@Getter
@Setter
@Validated
public class KafkaDemoProperties {
    @NonNull private String id;
    @NonNull private URL thirdpartyEndpoint;
    @NonNull private String outboundTopic;

    // A unique Id for this instance of the service.
    @NonNull private UUID instanceId = UUID.randomUUID();
}
