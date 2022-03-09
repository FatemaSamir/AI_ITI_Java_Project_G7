package com.example.Java_Project_G7;

import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class Test_Web_Service {
    @GetMapping("/hello")
    public String tes_fun(){
        return "Hello, Welcome to our Project java";
    }



}
