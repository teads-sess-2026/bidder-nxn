package com.teads.summerschool.creative;

import org.springframework.data.r2dbc.repository.R2dbcRepository;
import reactor.core.publisher.Flux;

public interface CreativeRepository extends R2dbcRepository<Creative, String> {

    Flux<Creative> findByBidderId(String bidderId); //sprongboots abstracts databases ; by justing writing a function
    //flux is for many, mono is for one


}
