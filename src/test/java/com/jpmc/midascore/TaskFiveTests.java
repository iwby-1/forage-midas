package com.jpmc.midascore;

import com.jpmc.midascore.foundation.Balance;
import com.jpmc.midascore.component.BalanceQuerier;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.test.annotation.DirtiesContext;

@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.DEFINED_PORT)
@DirtiesContext
@EmbeddedKafka(partitions = 1, brokerProperties = {"listeners=PLAINTEXT://localhost:9092", "port=9092"})
public class TaskFiveTests {
    static final Logger logger = LoggerFactory.getLogger(TaskFiveTests.class);

    @Autowired
    private KafkaProducer kafkaProducer;

    @Autowired
    private UserPopulator userPopulator;

    @Autowired
    private FileLoader fileLoader;

    @Autowired
    private BalanceQuerier balanceQuerier;

    @Test
    void task_five_verifier() throws InterruptedException {
        userPopulator.populate();
        String[] transactionLines = fileLoader.loadStrings("/test_data/rueiwoqp.tyruei");
        for (String transactionLine : transactionLines) {
            kafkaProducer.send(transactionLine);
        }

        Thread.sleep(10000);

        System.out.println("---begin output ---");
        for (long i = 0; i < 13; i++) {
            Balance balance = balanceQuerier.query(i);
            System.out.println(balance.toString());
        }
        System.out.println("---end output ---");
    }
}
