package com.itszt.controller;

import lombok.extern.slf4j.Slf4j;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.indices.PutMappingRequest;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import javax.annotation.Resource;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

@Slf4j
@RestController
@RequestMapping("/crud")
public class DocumentController {

    @Resource(name = "esConfig")
    private RestHighLevelClient client;

    @PutMapping("/put")
    public AcknowledgedResponse saveObject() throws IOException {
        PutMappingRequest request = new PutMappingRequest("user");

        Map<String, Object> jsonMap = new HashMap<>();
        Map<String, Object> message = new HashMap<>();
        message.put("type", "text");
        Map<String, Object> properties = new HashMap<>();
        properties.put("message", message);
        jsonMap.put("properties", properties);
        request.source(jsonMap);

        AcknowledgedResponse acknowledgedResponse = client.indices().putMapping(request, RequestOptions.DEFAULT);

        return acknowledgedResponse;
    }

}
