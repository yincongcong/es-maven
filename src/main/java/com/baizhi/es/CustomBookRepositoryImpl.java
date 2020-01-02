package com.baizhi.es;

import com.baizhi.entity.Book;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.fetch.subphase.highlight.HighlightBuilder;
import org.elasticsearch.search.fetch.subphase.highlight.HighlightField;
import org.elasticsearch.search.sort.SortBuilders;
import org.elasticsearch.search.sort.SortOrder;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Pageable;
import org.springframework.data.elasticsearch.core.ElasticsearchTemplate;
import org.springframework.data.elasticsearch.core.SearchResultMapper;
import org.springframework.data.elasticsearch.core.aggregation.AggregatedPage;
import org.springframework.data.elasticsearch.core.aggregation.impl.AggregatedPageImpl;
import org.springframework.data.elasticsearch.core.query.NativeSearchQuery;
import org.springframework.data.elasticsearch.core.query.NativeSearchQueryBuilder;
import org.springframework.data.elasticsearch.core.query.SearchQuery;
import org.springframework.stereotype.Repository;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;

@Repository
public class CustomBookRepositoryImpl implements CustomBookRepository{
    @Autowired
    private ElasticsearchTemplate elasticsearchTemplate;
    @Override
    public List<Book> findLast() {
        //高亮
        HighlightBuilder.Field field = new HighlightBuilder.Field("*").preTags("<span color = 'red'>").postTags("</span>").requireFieldMatch(false);
        NativeSearchQuery build = new NativeSearchQueryBuilder().withFilter(QueryBuilders.boolQuery().must(QueryBuilders.rangeQuery("price").lt(150)))
                .withQuery(QueryBuilders.queryStringQuery("小张")
                        .analyzer("ik_max_word")
                        .field("name")
                )
                .withSort(SortBuilders.fieldSort("price").order(SortOrder.ASC))
                .withPageable(PageRequest.of(0,5))
                .withFields("*")
                .withHighlightFields(field)
                .build();
        AggregatedPage<Book> books = elasticsearchTemplate.queryForPage((SearchQuery) build, Book.class, new SearchResultMapper() {
            @Override
            public <T> AggregatedPage<T> mapResults(SearchResponse searchResponse, Class<T> aClass, Pageable pageable) {
                ArrayList<Book> books = new ArrayList<>();
                SearchHit[] hits = searchResponse.getHits().getHits();
                //元数据
                for (SearchHit hit : hits) {
                    Book book = new Book();
                    Map<String, Object> sourceAsMap = hit.getSourceAsMap();
                    System.out.println(sourceAsMap);
                    Map<String, HighlightField> highlightFields = hit.getHighlightFields();
                    System.out.println("--------------------------");
                    if (sourceAsMap.get("id")!=null){
                        book.setId(sourceAsMap.get("id").toString());
                    }
                    if (sourceAsMap.get("name")!=null){
                        book.setName(sourceAsMap.get("name").toString());
                    }
                    if (sourceAsMap.get("price")!=null){
                        book.setPrice(Double.valueOf(sourceAsMap.get("price").toString()));
                    }
                    if (sourceAsMap.get("create_date")!=null){
                        book.setCreate_date(new Date(Long.valueOf(sourceAsMap.get("create_date").toString())));
                    }
                    if (sourceAsMap.get("des")!=null){
                        book.setDes(sourceAsMap.get("des").toString());
                    }
                    System.out.println(book);
                    System.out.println("--------------------------------");
                    //高亮
                    if (sourceAsMap.get("name")!=null){
                        if (highlightFields.get("name")!=null){
                            System.out.println(highlightFields.get("name").getFragments()[0].toString()+"============");
                            String name = highlightFields.get("name").getFragments()[0].toString();
                            book.setName(name);
                        }
                    }
                    if (sourceAsMap.get("des")!=null){
                        if (highlightFields.get("des")!=null){
                            System.out.println(highlightFields.get("des").getFragments()[0].toString()+"=============");
                            String name = highlightFields.get("des").getFragments()[0].toString();
                            book.setDes(name);
                        }
                    }
                    books.add(book);
                }
                return new AggregatedPageImpl<T>((List<T>) books);
            }
        });
        return books.getContent();
    }
}
