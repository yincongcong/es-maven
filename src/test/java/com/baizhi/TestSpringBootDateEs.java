package com.baizhi;

import com.baizhi.entity.Book;
import com.baizhi.es.BookRepository;
import com.baizhi.es.CustomBookRepository;
import com.baizhi.es.CustomBookRepositoryImpl;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Sort;
import org.springframework.test.context.junit4.SpringRunner;

import java.util.Date;
import java.util.List;
import java.util.Optional;

@SpringBootTest(classes = Application.class)
@RunWith(SpringRunner.class)
public class TestSpringBootDateEs {
    @Autowired
    private CustomBookRepository customBookRepository;
    @Autowired
    private BookRepository bookRepository;
    @Test//添加索引和更新索引   存在更新  不存在添加
    public void testSaveOrUpdate(){
        Book book = new Book();
        book.setId("2");
        book.setName("小张的故事");
        book.setCreate_date(new Date());
        book.setPrice(124.0);
        book.setDes("小张真的是一个好孩子");
        bookRepository.save(book);
    }
    @Test//删除一条索引
    public void testDelete(){
        Book book = new Book();
        book.setId("2");
        bookRepository.delete(book);
    }
    @Test//查询所有
    public void testFindAll(){
        Iterable<Book> books = bookRepository.findAll();
        for (Book book : books) {
            System.out.println(book);
        }
    }
    @Test//查询一个
    public void testFindOne(){
        Optional<Book> byId = bookRepository.findById("1");
        System.out.println(byId);
    }
    @Test//排序查询
    public void testFindAllOrder(){
        Iterable<Book> books = bookRepository.findAll(Sort.by(Sort.Order.asc("create_date")));
        for (Book book : books) {
            System.out.println(book);
        }
    }
    @Test//分页查询
    public void testPage(){
        Page<Book> books = bookRepository.findAll(PageRequest.of(0, 2));
        for (Book book : books) {
            System.out.println(book);
        }
    }
    @Test//通过名称和价格查
    public void findNameAndPrice(){
        List<Book> books = bookRepository.findByNameAndPrice("小陈的故事", 124.0);
        for (Book book : books) {
            System.out.println(book);
        }
    }
    @Test
    public void findLast(){
        List<Book> last = customBookRepository.findLast();
        for (Book book : last) {
            System.out.println(book+"***************");
        }
    }
}
