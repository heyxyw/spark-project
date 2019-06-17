package com.zhouq.sparkproject;

import com.zhouq.sparkproject.jdbc.JDBCHelper;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * JDBCHelper 组件测试
 *
 * @Author: zhouq
 * @Date: 2019-06-16
 */
public class JDBCHelperTest {

    private JDBCHelper jdbcHelper = null;

    @Before
    public void createJbdcHepler(){
        jdbcHelper = JDBCHelper.getInstance();
    }


    @Test
    public void executeUpdate(){
        jdbcHelper.executeUpdate("insert into test_user(name,age) values (?,?)",new Object[]{"heyxyw",18});
    }


    @Test
    public void executeQuery(){
        final Map<String,Object> testUser = new HashMap<String,Object>();

        jdbcHelper.executeQuery("select name,age from test_user where id =?", new Object[]{1}, rs -> {
            if (rs.next()){
                String name = rs.getString("name");
                int age = rs.getInt("age");
                testUser.put("name",name);
                testUser.put("age",age);
            }
        });

        System.out.println(testUser.get("name")+":"+testUser.get("age"));
    }


    @Test
    public void executeBatch(){
        String sql = "insert into test_user(name,age) values (?,?)";

        List<Object[]> paramsList = new ArrayList<>();

        paramsList.add(new Object[]{"zhouq",18});
        paramsList.add(new Object[]{"mazi",6});

        jdbcHelper.executeBatch(sql,paramsList);
    }

}
