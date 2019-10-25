package com.zhouq.sparkproject.dao.impl;

import com.zhouq.sparkproject.dao.ITop10CategoryDAO;
import com.zhouq.sparkproject.domain.Top10Category;
import com.zhouq.sparkproject.jdbc.JDBCHelper;

/**
 * top 10 品类 DAO 实现
 *
 * @Author: zhouq
 * @Date: 2019/10/25
 */
public class Top10CategoryDaoImpl implements ITop10CategoryDAO {
    @Override
    public void insert(Top10Category category) {
        String sql = "insert into top10_category values (?,?,?,?,?)";
        Object[] params = {category.getTaskid(),
                category.getCategoryid(),
                category.getClickCount(),
                category.getOrderCount(),
                category.getPayCount()};
        JDBCHelper jdbcHelper = JDBCHelper.getInstance();
        jdbcHelper.executeUpdate(sql, params);
    }
}
