package com.baizhi.es;

import com.baizhi.entity.Book;
import org.springframework.data.elasticsearch.repository.ElasticsearchRepository;

import java.util.List;

public interface BookRepository extends ElasticsearchRepository <Book ,String>{
    //通过姓名查询
    public List<Book> findByNameAndPrice(String name,Double price);
}
