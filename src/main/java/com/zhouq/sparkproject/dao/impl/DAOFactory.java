package com.zhouq.sparkproject.dao.impl;

import com.zhouq.sparkproject.dao.*;

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

    /**
     * 获取 ITop10CategoryDAO
     *
     * @return
     */
    public static ITop10CategoryDAO getTop10CategoryDAO() {
        return new Top10CategoryDaoImpl();
    }

    /**
     * 获取 ITop10SessionDAO
     *
     * @return
     */
    public static ITop10SessionDAO getTop10SessionDAO() {
        return new Top10SessionDAOImpl();
    }
}
