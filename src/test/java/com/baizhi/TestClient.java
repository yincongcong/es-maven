package com.baizhi;

import org.elasticsearch.action.admin.indices.create.CreateIndexResponse;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexResponse;
import org.elasticsearch.action.admin.indices.exists.indices.IndicesExistsResponse;
import org.elasticsearch.action.admin.indices.stats.IndexStats;
import org.elasticsearch.action.admin.indices.stats.IndicesStatsResponse;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.text.Text;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.aggregations.metrics.percentiles.hdr.InternalHDRPercentileRanks;
import org.elasticsearch.search.fetch.subphase.highlight.HighlightBuilder;
import org.elasticsearch.search.fetch.subphase.highlight.HighlightField;
import org.elasticsearch.search.sort.SortOrder;
import org.elasticsearch.transport.client.PreBuiltTransportClient;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;

public class TestClient {
    private TransportClient transportClient;
    @Before
    public void testClient() throws UnknownHostException {
        /*
         * 创建TransportAddress
         * 参数一：  和es连接的地址
         * 参数二： java与es通讯的端口
         * */
        TransportAddress transportAddress = new TransportAddress(InetAddress.getByName("192.168.26.13"),9300);
        /*
        * 创建客户端连接对象
        * 参数：集群相关设置（暂时为空，不设置）
        * */
        transportClient = new PreBuiltTransportClient(Settings.EMPTY).addTransportAddress(transportAddress);
    }
    @Test //获取集合中所有的节点
    public void test1(){
        List<DiscoveryNode> discoveryNodes = transportClient.listedNodes();
        for (DiscoveryNode discoveryNode : discoveryNodes) {
            System.out.println(discoveryNode);
        }
    }
    @Test//获取集合中所有索引
    public void test2(){
        IndicesStatsResponse indicesStatsResponse = transportClient.admin().indices().prepareStats().execute().actionGet();
        Map<String, IndexStats> indices = indicesStatsResponse.getIndices();
        Collection<IndexStats> values = indices.values();
        for (IndexStats value : values) {
            System.out.println(value.getIndex());
        }
    }
    @Test//创建索引
    public void test3(){
        CreateIndexResponse test = transportClient.admin().indices().prepareCreate("add").execute().actionGet();
        System.out.println(test.isAcknowledged());
    }
    @Test//删除索引
    public void test4(){
        DeleteIndexResponse add = transportClient.admin().indices().prepareDelete("add").execute().actionGet();
        System.out.println(add.isAcknowledged());
    }
    @Test//判断索引是否存在
    public void test5(){
        IndicesExistsResponse test = transportClient.admin().indices().prepareExists("test").execute().actionGet();
        System.out.println(test.isExists());
    }
    @Test//restful方式创建索引 ，类型并指定mapping
    public void test6() throws IOException {
        XContentBuilder xContentBuilder = XContentFactory.jsonBuilder();
        xContentBuilder.startObject()
                            .startObject("properties")
                                .startObject("name")
                                    .field("type","text")
                                    .field("analyzer","ik_max_word")
                                .endObject()
                                .startObject("age")
                                    .field("type","integer")
                                .endObject()
                                .startObject("sex")
                                    .field("type","keyword")
                                .endObject()
                                .startObject("content")
                                    .field("type","text")
                                    .field("analyzer","ik_max_word")
                                .endObject()
                            .endObject()
                       .endObject();
        /*
        * 参数1：索引
        * 参数2：类型
        * 参数3：类型对应的映射（要求json格式   用map集合比较繁琐）
        * */
        CreateIndexResponse createIndexResponse = transportClient.admin().indices().prepareCreate("dangdang").addMapping("book", xContentBuilder).execute().actionGet();
        System.out.println(createIndexResponse.isAcknowledged());
    }
    @Test//添加一条记录
    public void test7() throws IOException {
        /*
        * 创建索引（自动生成文档id）
        * */
        XContentBuilder xContentBuilder = XContentFactory.jsonBuilder().startObject()
                .field("name","rlh")
                .field("age",23)
                .field("sex","男")
                .field("content","这个中国人挺好的")
                .endObject();
        IndexResponse indexResponse = transportClient.prepareIndex("dangdang", "book").setSource(xContentBuilder).get();
        System.out.println(indexResponse.status());
    }
    @Test//更新一条记录
    public void test8() throws IOException {
        XContentBuilder xContentBuilder = XContentFactory.jsonBuilder();
        xContentBuilder.startObject().field("name","小黑是中国人").endObject();
        UpdateResponse updateResponse = transportClient.prepareUpdate("dangdang","book","PpyI_G4BLna6CVOm-ETO").setDoc(xContentBuilder).get();
        System.out.println(updateResponse.status());
    }
    @Test//删除一条记录
    public void test9(){
        DeleteResponse deleteResponse = transportClient.prepareDelete("dangdang", "book", "PpyI_G4BLna6CVOm-ETO").get();
        System.out.println(deleteResponse.status());
    }
    @Test//批量更新
    public void test10() throws IOException {
        //添加第一条记录
        IndexRequest indexRequest = new IndexRequest("dangdang", "book", "1");
        indexRequest.source(XContentFactory.jsonBuilder().startObject().field("name","中国科技").field("age",23).field("sex","男").field("content","这是个好人").endObject());
        //添加第二条记录
        IndexRequest indexRequest1 = new IndexRequest("dangdang", "book", "2");
        indexRequest1.source(XContentFactory.jsonBuilder().startObject().field("name","中国之声").field("age",25).field("sex","男").field("content","这是一个好声音").endObject());
        //更新一条记录
        UpdateRequest updateRequest = new UpdateRequest("dangdang", "book", "1");
        updateRequest.doc(XContentFactory.jsonBuilder().startObject().field("name","中国力量").endObject());
        //删除记录
        DeleteRequest deleteRequest = new DeleteRequest("dangdang", "book", "2");
        BulkResponse bulkItemResponses = transportClient.prepareBulk().add(indexRequest).add(indexRequest1).add(updateRequest).add(deleteRequest).get();
        BulkItemResponse[] items = bulkItemResponses.getItems();
        for (BulkItemResponse item : items) {
            System.out.println(item);
        }
    }
    @Test//查询所有并排序
    public void test11(){
        SearchResponse searchResponse = transportClient.prepareSearch("dangdang").setTypes("book").setQuery(QueryBuilders.matchAllQuery()).addSort("age", SortOrder.DESC).get();
        SearchHits hits = searchResponse.getHits();
        System.out.println("符合条件的记录条数："+hits.totalHits);
        for (SearchHit hit : hits) {
            System.out.println("当前索引的分数："+hit.getScore());
            System.out.println("对应结果：=======》"+hit.getSourceAsString());
            System.out.println("指定字段劫夺："+hit.getSourceAsMap().get("name"));
        }
    }
    @Test//分页查询
    public void test12(){
        SearchResponse searchResponse = transportClient.prepareSearch("dangdang").setTypes("book").setQuery(QueryBuilders.matchAllQuery()).setFrom(0).setSize(2).get();
        SearchHits hits = searchResponse.getHits();
        System.out.println("符合条件的记录："+hits.totalHits);
        for (SearchHit hit : hits) {
            System.out.println("当前索引的分数："+hit.getScore());
            System.out.println("对应结果："+hit.getSourceAsString());
            System.out.println("对应字段的结果："+hit.getSourceAsMap().get("name"));
        }
    }
    @Test//查询返回字段
    public void test13(){
        /*
        * 查询返回指定字段（source）默认返回所有
        *  setFetchSource 参数1：包含哪些字段   参数2：排除哪些字段
        *  setFetchSource（"*","age"） 返回所有字段中排除age字段
        *  setFetchSource（"name",""）  只返回name字段
        *  setFetchSource(new String[]{},new String[]{})
        * */
        SearchResponse searchResponse = transportClient.prepareSearch("dangdang").setTypes("book").setQuery(QueryBuilders.matchAllQuery()).setFetchSource("*", "age").get();
        SearchHits hits = searchResponse.getHits();
        System.out.println("符合条件的记录："+hits.totalHits);
        for (SearchHit hit : hits) {
            System.out.println("当前索引的分数："+hit.getScore());
            System.out.println("对应结果："+hit.getSourceAsString());
            System.out.println("对应字段的结果："+hit.getSourceAsMap().get("name"));
        }
    }
    @Test//term查询
    public void test14(){
        SearchResponse searchResponse = transportClient.prepareSearch("dangdang").setTypes("book").setQuery(QueryBuilders.termQuery("name", "中国")).get();
        SearchHits hits = searchResponse.getHits();
        System.out.println("符合条件的记录："+hits.totalHits);
        for (SearchHit hit : hits) {
            System.out.println("当前索引的分数："+hit.getScore());
            System.out.println("对应结果："+hit.getSourceAsString());
            System.out.println("对应字段的结果："+hit.getSourceAsMap().get("name"));
        }
    }
    @Test//range查询
    public void test15(){
        SearchResponse searchResponse = transportClient.prepareSearch("dangdang").setTypes("book").setQuery(QueryBuilders.rangeQuery("age").lt(45).gte(8)).get();
        SearchHits hits = searchResponse.getHits();
        System.out.println("符合条件的记录："+hits.totalHits);
        for (SearchHit hit : hits) {
            System.out.println("当前索引的分数："+hit.getScore());
            System.out.println("对应结果："+hit.getSourceAsString());
            System.out.println("对应字段的结果："+hit.getSourceAsMap().get("name"));
        }
    }
    @Test//prefix 查询
    public void test16(){
        SearchResponse searchResponse = transportClient.prepareSearch("dangdang").setTypes("book").setQuery(QueryBuilders.prefixQuery("name", "中")).get();
        SearchHits hits = searchResponse.getHits();
        System.out.println("符合条件的记录："+hits.totalHits);
        for (SearchHit hit : hits) {
            System.out.println("当前索引的分数："+hit.getScore());
            System.out.println("对应结果："+hit.getSourceAsString());
            System.out.println("对应字段的结果："+hit.getSourceAsMap().get("name"));
        }
    }
    @Test//wildcard查询
    public void test17(){
        SearchResponse searchResponse = transportClient.prepareSearch("dangdang").setTypes("book").setQuery(QueryBuilders.wildcardQuery("name", "中*")).get();
        SearchHits hits = searchResponse.getHits();
        System.out.println("符合条件的记录："+hits.totalHits);
        for (SearchHit hit : hits) {
            System.out.println("当前索引的分数："+hit.getScore());
            System.out.println("对应结果："+hit.getSourceAsString());
            System.out.println("对应字段的结果："+hit.getSourceAsMap().get("name"));
        }
    }
    @Test//ids查询
    public void test18(){
        SearchResponse searchResponse = transportClient.prepareSearch("dangdang").setTypes("book").setQuery(QueryBuilders.idsQuery().addIds("1")).get();
        SearchHits hits = searchResponse.getHits();
        System.out.println("符合条件的记录："+hits.totalHits);
        for (SearchHit hit : hits) {
            System.out.println("当前索引的分数："+hit.getScore());
            System.out.println("对应结果："+hit.getSourceAsString());
            System.out.println("对应字段的结果："+hit.getSourceAsMap().get("name"));
        }
    }
    @Test//fuzzy查询
    public void test19(){
        SearchResponse searchResponse = transportClient.prepareSearch("dangdang").setTypes("book").setQuery(QueryBuilders.fuzzyQuery("content", "国")).get();
        SearchHits hits = searchResponse.getHits();
        System.out.println("符合条件的记录："+hits.totalHits);
        for (SearchHit hit : hits) {
            System.out.println("当前索引的分数："+hit.getScore());
            System.out.println("对应结果："+hit.getSourceAsString());
            System.out.println("对应字段的结果："+hit.getSourceAsMap().get("name"));
        }
    }
    @Test//bool 查询
    public void test20(){
        BoolQueryBuilder boolQueryBuilder = QueryBuilders.boolQuery();
        boolQueryBuilder.should(QueryBuilders.matchAllQuery());
        boolQueryBuilder.mustNot(QueryBuilders.rangeQuery("age").lte(8));
        boolQueryBuilder.must(QueryBuilders.termQuery("name","中国"));
        SearchResponse searchResponse = transportClient.prepareSearch("dangdang").setTypes("book").setQuery(boolQueryBuilder).get();
        SearchHits hits = searchResponse.getHits();
        System.out.println("符合条件的记录："+hits.totalHits);
        for (SearchHit hit : hits) {
            System.out.println("当前索引的分数："+hit.getScore());
            System.out.println("对应结果："+hit.getSourceAsString());
            System.out.println("对应字段的结果："+hit.getSourceAsMap().get("name"));
        }
    }
    @Test//高亮查询
    public void test21() throws ExecutionException, InterruptedException {
        HighlightBuilder highlightBuilder = new HighlightBuilder();
        highlightBuilder.preTags("<span color='red'>");
        highlightBuilder.postTags("</span>");
        highlightBuilder.field("name");
        highlightBuilder.field("content");
        highlightBuilder.requireFieldMatch(false);
        SearchResponse searchResponse = transportClient.prepareSearch("dangdang").setTypes("book").setQuery(QueryBuilders.termQuery("name", "中国")).highlighter(highlightBuilder).execute().get();
        SearchHits hits = searchResponse.getHits();
        System.out.println("符合条件的记录："+hits.totalHits);
        for (SearchHit hit : hits) {
            //元数据
            Map<String, Object> sourceAsMap = hit.getSourceAsMap();
            System.out.println(sourceAsMap.values());
            //高亮字段
            Map<String, HighlightField> highlightFields = hit.getHighlightFields();
            Collection<HighlightField> values = highlightFields.values();
            for (HighlightField value : values) {
                Text fragment = value.getFragments()[0];
                System.out.println(fragment);
                System.out.println("==========高亮前=========");
                for (String s : sourceAsMap.keySet()) {
                    System.out.println(sourceAsMap.get(s));
                }
                System.out.println("============高亮后========");
                for (String s : sourceAsMap.keySet()) {
                    if (highlightFields.get(s)!=null){
                        System.out.println(highlightFields.get(s).getFragments()[0]);
                    }else {
                        System.out.println(sourceAsMap.get(s));
                    }
                }
            }
        }


    }
}
