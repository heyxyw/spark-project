package com.zhouq.sparkproject.dao;

import com.zhouq.sparkproject.domain.Top10Session;

/**
 * top10Session DAO
 *
 * @Author: zhouq
 * @Date: 2019/10/27
 */
public interface ITop10SessionDAO {
    /**
     * 插入 top10Session
     * @param top10Session
     */
    void insert(Top10Session top10Session);
}
