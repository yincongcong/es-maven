package com.baizhi.es;

import com.baizhi.entity.Book;

import java.util.List;

public interface CustomBookRepository {
    //使用过滤条件过滤价格在<=100  使用多字段分词查询 分页 排序 高亮 返回指定字段
    public List<Book> findLast();
}
