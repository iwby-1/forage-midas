package com.jpmc.midascore.component;

import com.jpmc.midascore.foundation.Balance;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.springframework.web.client.RestTemplate;

@Component
public class BalanceQuerier {

    @Autowired
    private RestTemplate restTemplate;

    public Balance query(Long id) {
        String url = "http://localhost:33400/balance?userId=" + id;
        return restTemplate.getForObject(url, Balance.class);
    }
}