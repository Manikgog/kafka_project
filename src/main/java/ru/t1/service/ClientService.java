package ru.t1.service;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import ru.t1.entity.Client;
import ru.t1.kafka.KafkaClientProducer;
import ru.t1.repository.ClientRepository;
import java.util.List;

@Slf4j
@Service
@RequiredArgsConstructor
public class ClientService {
    @Value("${t1.kafka.topic.client_id_registered}")
    private String clientRegistrationTopic;
    private final KafkaClientProducer kafkaClientProducer;
    private final ClientRepository repository;

    public void registerClients(List<Client> clients) {
        log.info("Registering clients... {}", clients);
        repository.saveAll(clients)
                .stream()
                .forEach(c -> kafkaClientProducer.send(clientRegistrationTopic, c));
    }

}
