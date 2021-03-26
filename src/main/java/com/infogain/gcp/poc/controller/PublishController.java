package com.infogain.gcp.poc.controller;

import com.infogain.gcp.poc.model.PNRModel;
import com.infogain.gcp.poc.service.PublishService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.*;

@RestController
public class PublishController {

    @Autowired
    private PublishService publishService;

    @PostMapping(value = "/api/v1/publish", consumes = MediaType.APPLICATION_JSON_VALUE, produces = MediaType.TEXT_PLAIN_VALUE)
    public String publishMessage(@RequestBody PNRModel pnrModel){
        return publishService.publish(pnrModel);
    }

    @GetMapping(value = "/api/v1/scheduler/job/publish", produces = MediaType.TEXT_PLAIN_VALUE)
    public String publishFailedRecords(){
        return publishService.publishFailedRecords();
    }

}