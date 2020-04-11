package com.itszt.controller;


import com.itszt.common.ResultData;
import com.itszt.domain.Customer;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.text.Text;
import org.elasticsearch.common.unit.Fuzziness;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.query.*;
import org.elasticsearch.index.reindex.BulkByScrollResponse;
import org.elasticsearch.index.reindex.DeleteByQueryRequest;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.fetch.subphase.highlight.HighlightBuilder;
import org.elasticsearch.search.fetch.subphase.highlight.HighlightField;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import javax.annotation.Resource;
import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

@RestController
@RequestMapping("/search")
public class SearchController {


    @Resource(name = "esConfig")
    private RestHighLevelClient client;

    /**
     * 查询指定索引的所有数据
     */
    @GetMapping("/matchall")
    public void search() throws IOException {
        SearchRequest searchRequest = new SearchRequest("customer");
        searchRequest.types("customer");
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        searchSourceBuilder.from(0);
        searchSourceBuilder.size(1000);
        //全部查询得
        MatchAllQueryBuilder matchAllQueryBuilder = QueryBuilders.matchAllQuery();
        searchSourceBuilder.query(matchAllQueryBuilder);
        SearchRequest result = searchRequest.source(searchSourceBuilder);

        SearchResponse search = client.search(result);

        SearchHits hits = search.getHits();

        long totalHits = hits.getTotalHits();
        System.out.println("totalHits = " + totalHits);

        SearchHit[] hits1 = hits.getHits();
        BufferedWriter bufferedWriter = new BufferedWriter(new FileWriter("C:\\Users\\86177\\Desktop\\customer.txt"));
        for (SearchHit hit : hits1) {
            //当前遍历id
//            hit.getId();
            //索引
//            hit.getIndex();
            //当前类型
//            hit.getType();
            Map<String, Object> sourceAsMap = hit.getSourceAsMap();
            Customer customer = new Customer();
            customer.setUsername(sourceAsMap.get("username").toString());
            customer.setSex(sourceAsMap.get("sex").toString());
            customer.setAge(Integer.parseInt(sourceAsMap.get("age").toString()));
            customer.setHome(sourceAsMap.get("home").toString());
            customer.setTime(sourceAsMap.get("birth").toString());
            bufferedWriter.write(customer.toString());
            bufferedWriter.newLine();
            bufferedWriter.flush();
        }
        bufferedWriter.close();
    }

    /**
     * 滚动API
     */
    @RequestMapping("/get")
    public void getscroll() throws IOException {

        SearchRequest searchRequest = new SearchRequest("customer");
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        searchSourceBuilder.query(QueryBuilders.matchQuery("age", 12));
        searchSourceBuilder.size(3);
        searchRequest.source(searchSourceBuilder);
        searchRequest.scroll(TimeValue.timeValueMinutes(1L));
        SearchResponse result = client.search(searchRequest);
        String scrollId = result.getScrollId();
        SearchHits hits = result.getHits();

        System.out.println("scrollId = " + scrollId);

        SearchHit[] hits1 = hits.getHits();

        for (SearchHit hit : hits1) {
            Map<String, Object> sourceAsMap = hit.getSourceAsMap();
            System.out.println("sourceAsMap = " + sourceAsMap);
        }
    }

    /**
     * 删除指定索引内的所有数据不包括索引
     *
     * @return
     * @throws IOException
     */
    @DeleteMapping("/delete")
    public BulkByScrollResponse deleteInfo() throws IOException {

        MatchAllQueryBuilder matchAllQueryBuilder = new MatchAllQueryBuilder();

        DeleteByQueryRequest request = new DeleteByQueryRequest("customer").setQuery(matchAllQueryBuilder);
        return client.deleteByQuery(request, RequestOptions.DEFAULT);

    }


    @GetMapping("/match")
    public void match() throws IOException {
        SearchRequest searchRequest = new SearchRequest("customer");
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();

        MatchQueryBuilder matchQueryBuilder = new MatchQueryBuilder("username", "dahuangda");
        //设置分词器
//        matchQueryBuilder.analyzer();
        //模糊查询
        matchQueryBuilder.fuzziness(Fuzziness.AUTO);
        //前缀查询长度
        matchQueryBuilder.prefixLength(3);
        //max expansion 选项 用来控制模糊查询
        matchQueryBuilder.maxExpansions(10);


        searchSourceBuilder.query(matchQueryBuilder);

        searchSourceBuilder.from(0);

        searchSourceBuilder.size(100);

        searchRequest.source(searchSourceBuilder);

        SearchResponse result = client.search(searchRequest);

        SearchHits hits = result.getHits();
        long totalHits = hits.getTotalHits();
        System.out.println("totalHits = " + totalHits);
        SearchHit[] hits1 = hits.getHits();
        for (SearchHit hit : hits1) {
            Map<String, Object> sourceAsMap = hit.getSourceAsMap();
            System.out.println("sourceAsMap = " + sourceAsMap);
        }

    }

    @GetMapping("/matchPhrase")
    public void matchPhrase() throws IOException {

        SearchRequest searchRequest = new SearchRequest("customer");
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        //完全按顺序匹配
        MatchPhraseQueryBuilder matchPhraseQueryBuilder = new MatchPhraseQueryBuilder("username", "this is a");
        matchPhraseQueryBuilder.boost(1.5f);
        //指定匹配词间可以有间隔词的个数
        matchPhraseQueryBuilder.slop(1);
        searchSourceBuilder.query(matchPhraseQueryBuilder);
        searchRequest.source(searchSourceBuilder);
        SearchResponse search = client.search(searchRequest, RequestOptions.DEFAULT);
        SearchHits hits = search.getHits();
        long totalHits = hits.getTotalHits();
        System.out.println("totalHits = " + totalHits);
        SearchHit[] hits1 = hits.getHits();
        for (SearchHit hit : hits1) {
            Map<String, Object> sourceAsMap = hit.getSourceAsMap();
            System.out.println("sourceAsMap = " + sourceAsMap);
        }
    }


    @GetMapping("/multimatch")
    public void multimatch() throws IOException {
        SearchRequest searchRequest = new SearchRequest("user");
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        //查询多个字段具有相同值的数据，只要有一个字段符合条件就作为结果集返回
        MultiMatchQueryBuilder multiMatchQueryBuilder = new MultiMatchQueryBuilder("haha321412", "name", "password");
        multiMatchQueryBuilder.field("password", 2);

        searchSourceBuilder.query(multiMatchQueryBuilder);
        searchRequest.source(searchSourceBuilder);

        SearchResponse search = client.search(searchRequest, RequestOptions.DEFAULT);
        SearchHits hits = search.getHits();
        long totalHits = hits.getTotalHits();
        System.out.println("totalHits = " + totalHits);
        SearchHit[] hits1 = hits.getHits();
        for (SearchHit hit : hits1) {
            Map<String, Object> sourceAsMap = hit.getSourceAsMap();
            System.out.println("sourceAsMap = " + sourceAsMap);
        }
    }


//    @GetMapping("/commonmatch")
//    public ResultData commmonMatch(){
//        SearchRequest searchRequest= new SearchRequest("user");
//        SearchSourceBuilder searchSourceBuilder= new SearchSourceBuilder();
//        CommonTermsQueryBuilder commonTermsQueryBuilder= new CommonTermsQueryBuilder();
//    }

    /**
     * 字符串查询
     *
     * @return
     * @throws IOException
     */
    @GetMapping("/querymatch")
    public ResultData queryString() throws IOException {
        ResultData resultData = new ResultData();
        SearchRequest searchRequest = new SearchRequest("customer");
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        //英文单词间有空格则会进行分词,只要有该词就会作为结果集返回
        QueryStringQueryBuilder queryStringQueryBuilder = new QueryStringQueryBuilder("not really");
        //默认是只要有其中至少一个分词就作为结果集,设置了and则必须都包含,与顺序无关
        queryStringQueryBuilder.defaultOperator(Operator.AND);
        queryStringQueryBuilder.defaultField("username");
        queryStringQueryBuilder.queryString();
        searchSourceBuilder.query(queryStringQueryBuilder);
        searchRequest.source(searchSourceBuilder);
        SearchResponse search = client.search(searchRequest);
        SearchHits hits = search.getHits();
        long totalHits = hits.getTotalHits();
        System.out.println("totalHits = " + totalHits);
        SearchHit[] hits1 = hits.getHits();
        List<Map<String, Object>> result = new ArrayList<>(10);
        for (SearchHit hit : hits1) {
            Map<String, Object> sourceAsMap = hit.getSourceAsMap();
            result.add(sourceAsMap);
        }

        resultData.setData(result);
        return resultData;

    }

    /**
     * 分词精确查询
     *
     * @return
     * @throws IOException
     */
    @GetMapping("/termquery")
    public ResultData termQuery() throws IOException {
        ResultData resultData = new ResultData();
        SearchRequest searchRequest = new SearchRequest("customer");
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();

        //在指定字段根据倒排索引查询包含指定分词的字段所在document文档
        //查询的必须是单个的分词
        TermQueryBuilder termQueryBuilder = new TermQueryBuilder("username", "really");
//        TermQueryBuilder termQueryBuilder = QueryBuilders.termQuery("username", "really");
        RangeQueryBuilder rangeQueryBuilder = QueryBuilders.rangeQuery("age").gte(1).lte(100);
        ExistsQueryBuilder existsQueryBuilder = QueryBuilders.existsQuery("sex");
        IdsQueryBuilder idsQueryBuilder = QueryBuilders.idsQuery().addIds("9d460827aa3844f3a0660bb07857f813", "d2ff6410da3a4b4babe36c0b46fdfeeb");
        termQueryBuilder.boost(2f);
        searchSourceBuilder.query(rangeQueryBuilder);
        searchSourceBuilder.query(termQueryBuilder);
        searchSourceBuilder.query(idsQueryBuilder);
        searchRequest.source(searchSourceBuilder);
        SearchResponse search = client.search(searchRequest);
        SearchHits hits = search.getHits();
        long totalHits = hits.getTotalHits();
        System.out.println("totalHits = " + totalHits);
        List<Map<String, Object>> result = new ArrayList<>(10);
        SearchHit[] hits1 = hits.getHits();
        for (SearchHit hit : hits1) {
            Map<String, Object> sourceAsMap = hit.getSourceAsMap();
            System.out.println("sourceAsMap = " + sourceAsMap);
            result.add(sourceAsMap);
        }
        resultData.setData(result);
        return resultData;
    }

    /**
     * 设置查询字段的高亮
     *
     * @return
     * @throws IOException
     */
    @GetMapping("/highlight")
    public ResultData hightlight() throws IOException {
        ResultData resultData = new ResultData();
        SearchRequest searchRequest = new SearchRequest("customer");
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        MatchAllQueryBuilder matchAllQueryBuilder = QueryBuilders.matchAllQuery();
        searchSourceBuilder.query(matchAllQueryBuilder);
        HighlightBuilder highlightBuilder = new HighlightBuilder().field("*").requireFieldMatch(false);
        highlightBuilder.preTags("<span style=\"color:red\">");
        highlightBuilder.postTags("</span>");
        //下面这两项,如果你要高亮如文字内容等有很多字的字段,必须配置,不然会导致高亮不全,文章内容缺失等
        highlightBuilder.fragmentSize(800000); //最大高亮分片数
        highlightBuilder.numOfFragments(0);
//        HighlightBuilder highlightBuilder = new HighlightBuilder();
//        //设置高亮的字段
//        highlightBuilder.field("username");
//        //设置高亮类型
//        highlightBuilder.highlighterType("unified");
        searchSourceBuilder.highlighter(highlightBuilder);
        searchRequest.source(searchSourceBuilder);
        SearchResponse search = client.search(searchRequest);
        SearchHits hits = search.getHits();
        long totalHits = hits.getTotalHits();
        System.out.println("totalHits = " + totalHits);
        SearchHit[] hits1 = hits.getHits();
        List<Map<String, Object>> result = new ArrayList<>();
        for (SearchHit hit : hits1) {
            Map<String, Object> sourceAsMap = hit.getSourceAsMap();

            HighlightField username = hit.getHighlightFields().get("home");
            if (username != null) {
                Text[] fragments = username.fragments();
                String text = "";
                for (Text fragment : fragments) {
                    text += fragment;
                }
                System.out.println("text = " + text);
                sourceAsMap.put("username", text);
            }


            System.out.println("sourceAsMap = " + sourceAsMap);

            result.add(sourceAsMap);
        }
        resultData.setData(result);
        return resultData;
    }

    /**
     * 复合查询
     * @return
     * @throws IOException
     */
    @GetMapping("/boolquery")
    public ResultData boolQuery() throws IOException {
        ResultData resultData = new ResultData();
        SearchRequest searchRequest = new SearchRequest("customer");
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        BoolQueryBuilder boolQueryBuilder = QueryBuilders.boolQuery();
        boolQueryBuilder.must(QueryBuilders.matchPhraseQuery("username", "this"));
        boolQueryBuilder.filter(QueryBuilders.rangeQuery("age").gte(900));
        boolQueryBuilder.mustNot(QueryBuilders.matchQuery("home", "德州"));
        boolQueryBuilder.should(QueryBuilders.matchQuery("sex", "man"));
        searchSourceBuilder.query(boolQueryBuilder);
        searchRequest.source(searchSourceBuilder);
        SearchResponse search = client.search(searchRequest);
        SearchHits hits = search.getHits();
        long totalHits = hits.getTotalHits();
        System.out.println("totalHits = " + totalHits);
        SearchHit[] hits1 = hits.getHits();
        List<Map<String, Object>> result = new ArrayList<>();
        for (SearchHit hit : hits1) {
            Map<String, Object> sourceAsMap = hit.getSourceAsMap();
            System.out.println("sourceAsMap = " + sourceAsMap);
            result.add(sourceAsMap);
        }
        resultData.setData(result);
        return resultData;
    }
}



