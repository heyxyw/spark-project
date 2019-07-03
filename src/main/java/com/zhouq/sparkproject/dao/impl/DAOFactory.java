package com.zhouq.sparkproject.dao.impl;

import com.zhouq.sparkproject.dao.ISessionAggrStatDAO;
import com.zhouq.sparkproject.dao.ISessionDetailDAO;
import com.zhouq.sparkproject.dao.ISessionRandomExtractDAO;
import com.zhouq.sparkproject.dao.ITaskDao;

/**
 * DAO 工厂类
 */
public class DAOFactory {

    /**
     * 获取任务管理DAO
     *
     * @return
     */
    public static ITaskDao getTaskDao() {
        return new TaskDaoImpl();
    }

    /**
     * 获取session 聚合 管理 DAO
     *
     * @return
     */
    public static ISessionAggrStatDAO getSessionAggrStatDAO() {
        return new SessionAggrStatDAOImpl();
    }

    /**
     * 获取随机抽取 session DAO
     *
     * @return
     */
    public static ISessionRandomExtractDAO getSessionRandomExtractDAO() {
        return new SessionRandomExtractDAOImpl();
    }

    /**
     * 获取 SessionDetail DAO
     *
     * @return
     */
    public static ISessionDetailDAO getSessionDetailDAO() {
        return new SessionDetailDAOImpl();
    }

}
