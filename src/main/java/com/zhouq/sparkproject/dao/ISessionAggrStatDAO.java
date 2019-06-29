package com.zhouq.sparkproject.dao;

import com.zhouq.sparkproject.domain.SessionAggrStat;

/**
 * session 聚合统计模块DAO 接口
 * Create by zhouq on 2019/6/29
 */
public interface ISessionAggrStatDAO {

    /**
     * 插入 session 聚合统计结果
     *
     * @param sessionAggrStat
     */
    void insert(SessionAggrStat sessionAggrStat);
}
