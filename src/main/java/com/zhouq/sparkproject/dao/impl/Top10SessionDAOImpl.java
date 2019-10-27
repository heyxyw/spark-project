package com.zhouq.sparkproject.dao.impl;

import com.zhouq.sparkproject.dao.ITop10SessionDAO;
import com.zhouq.sparkproject.domain.Top10Session;
import com.zhouq.sparkproject.jdbc.JDBCHelper;

/**
 * Top10Session DAO 实现
 *
 * @Author: zhouq
 * @Date: 2019/10/27
 */
public class Top10SessionDAOImpl implements ITop10SessionDAO {
    @Override
    public void insert(Top10Session top10Session) {
        String sql = "insert into top10_session values (?,?,?,?)";

        Object[] params = new Object[]{
                top10Session.getTaskid(),
                top10Session.getCategoryid(),
                top10Session.getSessionid(),
                top10Session.getClickCount()
        };

        JDBCHelper jdbcHelper = JDBCHelper.getInstance();
        jdbcHelper.executeUpdate(sql, params);

    }
}
