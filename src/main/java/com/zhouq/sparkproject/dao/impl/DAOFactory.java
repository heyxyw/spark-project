package com.zhouq.sparkproject.dao.impl;

import com.zhouq.sparkproject.dao.ITaskDao;

/**
 * DAO 工厂类
 */
public class DAOFactory {

    /** 获取任务管理DAO
     * @return
     */
    public static ITaskDao getTaskDao() {
        return new TaskDaoImpl();
    }
}