package ru.t1.config;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.*;
import ru.t1.dto.ClientDto;
import ru.t1.entity.Client;
import ru.t1.kafka.KafkaClientProducer;
import ru.t1.kafka.MessageDeserializer;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.*;
import org.springframework.kafka.listener.CommonErrorHandler;
import org.springframework.kafka.listener.ContainerProperties;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.kafka.support.serializer.ErrorHandlingDeserializer;
import org.springframework.kafka.support.serializer.JsonDeserializer;
import org.springframework.kafka.support.serializer.JsonSerializer;
import org.springframework.util.backoff.FixedBackOff;

import java.util.HashMap;
import java.util.Map;

@Slf4j
@Configuration
public class KafkaConfig {

    @Value("${spring.kafka.consumer.group-id}")
    private String groupId;
    @Value("${t1.kafka.consumer.group-id}")
    private String groupId1;
    @Value("${spring.kafka.producer.bootstrap-servers}")
    private String servers;
    @Value("${t1.kafka.session.timeout.ms:15000}")
    private String sessionTimeout;
    @Value("${t1.kafka.max.partition.fetch.bytes:300000}")
    private String maxPartitionFetchBytes;
    @Value("${t1.kafka.max.poll.records:1}")
    private String maxPollRecords;
    @Value("${t1.kafka.max.poll.interval.ms:3000}")
    private String maxPollIntervalsMs;
    @Value("${t1.kafka.topic.client_id_registered}")
    private String clientTopic;
    @Value("${t1.kafka.topic.client_registration}")
    private String clientIdTopic;

    @Bean
    public ConsumerFactory<String, ClientDto> consumerListenerFactory() {
        Map<String, Object> props = new HashMap<>();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, servers); // сервер
        props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId); // консюмер группа
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class); // кто будет десериализовать ключ
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, MessageDeserializer.class); // кто будет десериализовать value
        props.put(JsonDeserializer.VALUE_DEFAULT_TYPE, "ru.t1.dto.ClientDto"); // во что маппим? где это взять
        props.put(JsonDeserializer.TRUSTED_PACKAGES, "*"); //если маппим во что-то вне пакета - кафка не сделает этого,
        props.put(JsonDeserializer.USE_TYPE_INFO_HEADERS, false); // заголовки
        props.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, sessionTimeout);
        props.put(ConsumerConfig.MAX_PARTITION_FETCH_BYTES_CONFIG, maxPartitionFetchBytes); // максимальный размер сообщения, если не уложится будет частично доставлено
        props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, maxPollRecords); // сколько сообщений прочитать за один раз и коммит одного оффсета
        props.put(ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG, maxPollIntervalsMs); // время сколько консьюмер может получать ответ от кафки и работать, если упал - считается что умер, в консьмер группе перебалансируется
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, Boolean.FALSE); // консьюмер будет ли автоматически подтверждать смещение после обработки сообщения, выкл - делаем сами, и это best practise
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest"); // начинать с раннего сообщения
        props.put(ErrorHandlingDeserializer.VALUE_DESERIALIZER_CLASS, MessageDeserializer.class.getName()); // для ошибок свой десериализатор
        props.put(ErrorHandlingDeserializer.KEY_DESERIALIZER_CLASS, MessageDeserializer.class); // для ошибок свой десериализатор

        DefaultKafkaConsumerFactory<String, ClientDto> factory = new DefaultKafkaConsumerFactory<String, ClientDto>(props);
        factory.setKeyDeserializer(new StringDeserializer());

        return factory;
    }

    @Bean
    ConcurrentKafkaListenerContainerFactory<String, ClientDto> kafkaListenerContainerFactory(@Qualifier("consumerListenerFactory") ConsumerFactory<String, ClientDto> consumerFactory) {
        ConcurrentKafkaListenerContainerFactory<String, ClientDto> factory = new ConcurrentKafkaListenerContainerFactory<>();
        factoryBuilder(consumerFactory, factory);
        return factory;
    }


    @Bean
    public ConsumerFactory<String, Client> consumerListenerFactory1() {
        Map<String, Object> props = new HashMap<>();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, servers); // сервер
        props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId1); // консюмер группа
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class); // кто будет десериализовать ключ
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, JsonDeserializer.class); // кто будет десериализовать value
        props.put(JsonDeserializer.VALUE_DEFAULT_TYPE, "ru.t1.entity.Client"); // во что маппим? где это взять
        props.put(JsonDeserializer.TRUSTED_PACKAGES, "ru.t1"); //если маппим во что-то вне пакета - кафка не сделает этого,
        props.put(JsonDeserializer.USE_TYPE_INFO_HEADERS, false); // заголовки
        props.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, sessionTimeout);
        props.put(ConsumerConfig.MAX_PARTITION_FETCH_BYTES_CONFIG, maxPartitionFetchBytes); // максимальный размер сообщения, если не уложится будет частично доставлено
        props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, maxPollRecords); // сколько сообщений прочитать за один раз и коммит одного оффсета
        props.put(ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG, maxPollIntervalsMs); // время сколько консьюмер может получать ответ от кафки и работать, если упал - считается что умер, в консьмер группе перебалансируется
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, Boolean.FALSE); // консьюмер будет ли автоматически подтверждать смещение после обработки сообщения, выкл - делаем сами, и это best practise
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest"); // начинать с раннего сообщения
        props.put(ErrorHandlingDeserializer.VALUE_DESERIALIZER_CLASS, JsonDeserializer.class.getName()); // для ошибок свой десериализатор
        props.put(ErrorHandlingDeserializer.KEY_DESERIALIZER_CLASS, MessageDeserializer.class); // для ошибок свой десериализатор

        DefaultKafkaConsumerFactory<String, Client> factory = new DefaultKafkaConsumerFactory<String, Client>(props);
        factory.setKeyDeserializer(new StringDeserializer());

        return factory;
    }

    @Bean
    ConcurrentKafkaListenerContainerFactory<String, Client> kafkaListenerContainerFactory1(@Qualifier("consumerListenerFactory1") ConsumerFactory<String, Client> consumerFactory) {
        ConcurrentKafkaListenerContainerFactory<String, Client> factory = new ConcurrentKafkaListenerContainerFactory<>();
        factoryBuilder(consumerFactory, factory);
        return factory;
    }

    private <T> void factoryBuilder(ConsumerFactory<String, T> consumerFactory, ConcurrentKafkaListenerContainerFactory<String, T> factory) {
        factory.setConsumerFactory(consumerFactory);
        factory.setBatchListener(true);
        factory.setConcurrency(1);
        factory.getContainerProperties().setAckMode(ContainerProperties.AckMode.MANUAL_IMMEDIATE);
        factory.getContainerProperties().setPollTimeout(5000);
        factory.getContainerProperties().setMicrometerEnabled(true);
        factory.setCommonErrorHandler(errorHandler());
    }

    // для повторной попытки почитать сообщения
    private CommonErrorHandler errorHandler() {
        DefaultErrorHandler handler = new DefaultErrorHandler(new FixedBackOff(1000, 3));
        handler.addNotRetryableExceptions(IllegalStateException.class);
        handler.setRetryListeners((record, ex, deliveryAttempt) -> {
            log.error(" RetryListeners message = {}, offset = {} deliveryAttempt = {}", ex.getMessage(), record.offset(), deliveryAttempt);
        });
        return handler;
    }

    @Bean("client")
    public KafkaTemplate<String, ClientDto> kafkaTemplate(ProducerFactory<String, ClientDto> producerPatFactory) {
        return new KafkaTemplate<>(producerPatFactory);
    }

    @Bean
    @ConditionalOnProperty(value = "t1.kafka.producer.enable",
            havingValue = "true",
            matchIfMissing = true)
    public KafkaClientProducer producerClient(@Qualifier("client") KafkaTemplate<String, ClientDto> template, @Qualifier("client-id") KafkaTemplate<String, Client> template1) {
        template.setDefaultTopic(clientTopic);
        template1.setDefaultTopic(clientIdTopic);
        return new KafkaClientProducer(template, template1);
    }

    @Bean
    public ProducerFactory<String, ClientDto> producerClientFactory() {
        Map<String, Object> props = new HashMap<>();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, servers);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, JsonSerializer.class);
        props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, false);
        return new DefaultKafkaProducerFactory<>(props);
    }

    @Bean("client-id")
    public KafkaTemplate<String, Client> kafkaTemplate1(ProducerFactory<String, Client> producerPatFactory) {
        return new KafkaTemplate<>(producerPatFactory);
    }

    @Bean
    public ProducerFactory<String, Client> producerClientFactory1() {
        Map<String, Object> props = new HashMap<>();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, servers);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, JsonSerializer.class);
        props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, false);
        return new DefaultKafkaProducerFactory<>(props);
    }

}