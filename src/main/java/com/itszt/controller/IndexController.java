package com.itszt.controller;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.itszt.domain.Customer;
import com.itszt.domain.IndexParmers;
import com.itszt.domain.User;
import com.sun.org.apache.regexp.internal.RE;
import lombok.extern.slf4j.Slf4j;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.admin.indices.mapping.put.PutMappingRequestBuilder;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.Index;
import org.springframework.asm.ClassTooLargeException;
import org.springframework.web.bind.annotation.*;

import javax.annotation.Resource;
import java.io.IOException;
import java.sql.BatchUpdateException;
import java.sql.Timestamp;
import java.time.LocalDateTime;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import java.util.UUID;

@Slf4j
@RestController
@RequestMapping(value = "/index")
public class IndexController {

    private static String[] homes = {"北京", "上海", "武汉", "衡水", "青岛", "德州", "天津", "珠海", "哈尔滨"};

    private static String[] names = {"damao", "dagou", "daxiang", "dahouzi", "dameizi", "damuji", "dageda", "xinderen", "dagezi", "darenwu", "daqili", "darenwu", "dagedaqilaide"};

    @Resource(name = "esConfig")
    private RestHighLevelClient client;

    @PostMapping(value = "/create")
    public void createIndex(@RequestBody IndexParmers parmers) throws IOException, InterruptedException {

        IndexRequest indexRequest = new IndexRequest(parmers.getIndex(), parmers.getType(), UUID.randomUUID().toString());

        Map<String, Object> jsonMap = new HashMap<>();

        jsonMap.put("username", "heihei");
        jsonMap.put("password", "123456");
        jsonMap.put("age", 1232);
        jsonMap.put("postDate", LocalDateTime.now());


        indexRequest.source(jsonMap);
        indexRequest.routing("wife121");

        indexRequest.timeout(TimeValue.timeValueMinutes(1));

//        IndexResponse indexResponse = client.index(indexRequest, RequestOptions.DEFAULT);

        ActionListener<IndexResponse> listener = new ActionListener<IndexResponse>() {
            @Override
            public void onResponse(IndexResponse indexResponse) {
                System.out.println("成功！！！");
            }

            @Override
            public void onFailure(Exception e) {
                System.out.println("失败！！！");

                System.out.println("e = " + e);

            }
        };

        client.indexAsync(indexRequest, RequestOptions.DEFAULT, listener);

        Thread.sleep(1000);
//        return indexResponse;
    }


    @DeleteMapping("/delete/{index}")
    public AcknowledgedResponse delete(@PathVariable String index) throws IOException {
        log.info("enter delete index:{}", index);
        DeleteIndexRequest request = new DeleteIndexRequest(index);
        request.timeout(TimeValue.timeValueMinutes(2));
        request.masterNodeTimeout(TimeValue.timeValueMinutes(1));
        request.indicesOptions(IndicesOptions.LENIENT_EXPAND_OPEN);
        AcknowledgedResponse delete = client.indices().delete(request, RequestOptions.DEFAULT);
        return delete;
    }

    @PutMapping("/add")
    public IndexResponse add() throws IOException {
        IndexRequest request = new IndexRequest("customer", "customer", UUID.randomUUID().toString());

        XContentBuilder builder = XContentFactory.jsonBuilder();
        builder.startObject();
        {
            builder.field("username", "lijinjian");
            builder.field("sex", "man");
            builder.field("age", 27);
            builder.field("home", "beijing");
        }
        builder.endObject();
        //路由值
        request.routing("routing");

        request.source(builder);
        request.timeout(TimeValue.timeValueMinutes(2));
        request.opType("create");

        return client.index(request);

    }

    /**
     * 批量添加数据bulk
     */
    @PutMapping("/bulk/{name}")
    public void bulkRequest(@PathVariable String name) throws IOException {
        BulkRequest request = new BulkRequest();
//        request.add(new IndexRequest("customer", "user", UUID.randomUUID().toString())
//                .source(XContentType.JSON, "username", "haha", "age", "24", "sex", "women"));
//        request.add(new IndexRequest("customer", "user", UUID.randomUUID().toString())
//                .source(XContentType.JSON, "username", "heihei", "age", "35", "sex", "man"));
//        request.add(new IndexRequest("customer", "user", UUID.randomUUID().toString())
//                .source(XContentType.JSON, "username", "hengheng", "age", "21", "sex", "women"));
//        for (int i = 0; i < 2; i++) {
        String id = UUID.randomUUID().toString().replaceAll("-", "");
        IndexRequest requestObject = new IndexRequest("customer", "customer", id);
        Customer customer = new Customer();
        customer.setAge(new Random().nextInt(10000));
//            if (i % 2 == 0) {
        customer.setSex("women");
//            } else {
//                customer.setSex("man");
//            }
        customer.setBirth(LocalDateTime.now());
//            customer.setUsername(names[new Random().nextInt(names.length)] + "dahuang" + names[new Random().nextInt(names.length)]);
//            customer.setUsername("dahuang" + names[new Random().nextInt(names.length)]);
        customer.setUsername(name);
        customer.setHome(homes[new Random().nextInt(homes.length)]);
        Timestamp timestamp = Timestamp.valueOf(customer.getBirth());
        long time = timestamp.getTime();
        requestObject.source(XContentType.JSON, "username", customer.getUsername(), "sex", customer.getSex(), "age", customer.getAge(), "home", customer.getHome(), "birth", time);
        request.add(requestObject);
//        }


        request.timeout(TimeValue.timeValueMinutes(2));
        request.setRefreshPolicy("wait_for");
        //设置当索引数据更新前必须活跃的副本
        request.waitForActiveShards(2);
        client.bulk(request);


    }

    //    @PutMapping("/insert")
//    public void insert() throws IOException {
//
//        User user = new User();
//        user.setName("haha");
//        user.setAge(25);
//        user.setCard("43214213412421");
//        user.setBirth(LocalDateTime.now());
//        user.setScore(14321412);
//        user.setSex(1);
//        user.setHome("北京");
//
//
//    }
    @PutMapping("/insert")
    public void addfiled() throws IOException {
        IndexRequest request = new IndexRequest("user", "user", "2");
        Map<String, Object> jsonMap = new HashMap<>();
        jsonMap.put("name", "haha");
        jsonMap.put("password", "heihei");
        request.source(jsonMap);
        client.index(request, RequestOptions.DEFAULT);
    }
}
