package com.infogain.gcp.poc.controller;

import com.infogain.gcp.poc.service.PublishService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class PublishController {

    @Autowired
    private PublishService publishService;

    @GetMapping(value = "/api/v1/publish/{pnrId}", produces = MediaType.TEXT_PLAIN_VALUE)
    public String publishMessage(@PathVariable("pnrId") String pnrId){
        return publishService.publish(pnrId);
    }

    @GetMapping(value = "/api/v1/publish", produces = MediaType.TEXT_PLAIN_VALUE)
    public String publishFailedRecords(){
        return publishService.publishFailedRecords();
    }

}