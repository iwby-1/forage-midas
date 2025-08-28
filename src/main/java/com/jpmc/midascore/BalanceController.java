package com.jpmc.midascore;

import com.jpmc.midascore.entity.UserRecord;
import com.jpmc.midascore.foundation.Balance;
import com.jpmc.midascore.repository.UserRepository;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import java.util.Optional;

@RestController
public class BalanceController {

    @Autowired
    private UserRepository userRepository;

    @GetMapping("/balance")
    public Balance getBalance(@RequestParam Long userId) {
        Optional<UserRecord> userRecordOptional = userRepository.findById(userId);
        if (userRecordOptional.isPresent()) {
            UserRecord user = userRecordOptional.get();
            return new Balance(user.getBalance());
        }
        return new Balance(0.0f);
    }
}