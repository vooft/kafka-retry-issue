package com.example.kafkaissue;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.RetryingBatchErrorHandler;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.util.backoff.FixedBackOff;

import java.time.Duration;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.awaitility.Awaitility.await;

@EnableKafka
@EnableConfigurationProperties(KafkaProperties.class)
@EnableAutoConfiguration
@SpringBootTest
@Import({KafkaIssueTest.TestConfig.class, KafkaIssueTest.TestContainerFactoryConfig.class})
@EmbeddedKafka(topics = {KafkaIssueTest.TOPIC})
class KafkaIssueTest {

	private static final Logger log = LoggerFactory.getLogger(KafkaIssueTest.class);

    public static final String TOPIC = "test-topic";
	public static final String GROUP = "test-group";

	private static final Duration TIMEOUT = Duration.ofMillis(50);

	private static final int TOTAL_RECORDS = 50;
	private static final Set<String> REMAINING = Collections.synchronizedSet(new HashSet<>());
	private static final AtomicBoolean SPRING_KAFKA_LISTENER_FAILURE_FLAG = new AtomicBoolean(true);

	@Value("${spring.embedded.kafka.brokers}")
	private String brokerAddresses;

	@Autowired
	private KafkaTemplate<String, String> kafkaTemplate;

	@Configuration
	static class TestConfig {
		@KafkaListener(topics = TOPIC, groupId = GROUP, containerFactory = "testContainerFactory")
		public void listen(List<ConsumerRecord<String, String>> batch) {
			if (SPRING_KAFKA_LISTENER_FAILURE_FLAG.get()) {
				throw new RuntimeException("just failing");
			} else {
				for (ConsumerRecord<String, String> record : batch) {
					log.info("Processed by spring kafka listener {}", record.value());
					REMAINING.remove(record.value());
				}
			}
		}
	}

	@Configuration
	static class TestContainerFactoryConfig {
		@Bean
		public ConcurrentKafkaListenerContainerFactory<String, Object> testContainerFactory(KafkaProperties kafkaProperties) {
			final var consumerFactory = new DefaultKafkaConsumerFactory<>(kafkaProperties.buildConsumerProperties());

			final var containerFactory = new ConcurrentKafkaListenerContainerFactory<String, Object>();
			containerFactory.setConsumerFactory(consumerFactory);
			containerFactory.setBatchListener(true);

			final var backoff = new FixedBackOff(250, Long.MAX_VALUE); // may need to reduce the backoff if not failing

			final var errorHandler = new RetryingBatchErrorHandler(backoff, (ignore1, ignore2) -> {
				throw new RuntimeException("boo");
			});

			// will work if replaced with SeekToCurrentBatchErrorHandler
//			final var errorHandler = new SeekToCurrentBatchErrorHandler();
//			errorHandler.setBackOff(backoff);
			containerFactory.setBatchErrorHandler(errorHandler);
			return containerFactory;
		}
	}


    @Test
    void test() throws Exception {
		for (int i = 0; i < TOTAL_RECORDS; i++) {
			final var sendResult = kafkaTemplate.send(TOPIC, String.valueOf(i), String.valueOf(i)).get();
			log.info("Sent record {}", sendResult.getRecordMetadata());
			REMAINING.add(String.valueOf(i));
		}

		Thread.sleep(1000);

		// create second consumer to force rebalancing
		final var consumer = createConsumer();
		consumer.subscribe(List.of(TOPIC));

		int maxEmpty = 30;
		while (maxEmpty > 0) {
			final var batch = consumer.poll(TIMEOUT);
			if (batch.isEmpty()) {
				maxEmpty--;
			}
			for (ConsumerRecord<String, String> record : batch) {
				log.info("Received by manual consumer {}", record.value());
				REMAINING.remove(record.value());
			}

			Thread.sleep(TIMEOUT.toMillis());
		}

		SPRING_KAFKA_LISTENER_FAILURE_FLAG.set(false);

		await().atMost(Duration.ofSeconds(10)).until(() -> REMAINING, Set::isEmpty);
    }

	private KafkaConsumer<String, String> createConsumer() {
		final var props = new Properties();
		props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, brokerAddresses);
		props.put(ConsumerConfig.GROUP_ID_CONFIG, GROUP);
		props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
		props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
		props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 1);
		props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

		return new KafkaConsumer<>(props);
	}
}
