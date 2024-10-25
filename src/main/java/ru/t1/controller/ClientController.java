package ru.t1.controller;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import ru.t1.dto.ClientDto;
import ru.t1.kafka.KafkaClientProducer;
import ru.t1.service.ClientService;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;
import ru.t1.service.ClientsGenerator;

import java.util.List;

@RestController
@RequiredArgsConstructor
@Slf4j
public class ClientController {

    private final ClientService clientService;
    private final KafkaClientProducer kafkaClientProducer;
    private final ClientsGenerator clientsGenerator;

    @Value("${t1.kafka.topic.client_registration}")
    private String topic;


    @GetMapping(value = "/parse")
    public void parseSource() {
        List<ClientDto> clientDtos = clientsGenerator.generate(5L);
        clientDtos.forEach(dto -> kafkaClientProducer.sendTo(topic, dto));
    }
}
