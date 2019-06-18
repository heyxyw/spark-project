package com.zhouq.sparkproject.constant;

/**
 * //todo
 *
 * @Author: zhouq
 * @Date: 2019-06-16
 */
public interface Constants {
    /**
     * 数据库配置常量
     */
    String JDBC_DRIVER = "jdbc.driver";
    /**
     * 数据库连接池大小
     */
    String JDBC_DATASOURCE_SIZE = "jdbc.datasource.size";
    /**
     * 数据库地址
     */
    String JDBC_URL = "jdbc.url";
    /**
     * 用户名
     */
    String JDBC_USER = "jdbc.user";

    /**
     * 密码
     */
    String JDBC_PASSWORD = "jdbc.password";

    /**
     * Spark 作业名称
     */
    String SPARK_APP_NAME_SESSION = "UserVisitSessionAnalyzeSpark";

    /**
     * spark 作业本地模式
     */
    String SPARK_LOCAL = "spark.local";


    /**
     * 参数相关的常量
     */
    String PARAM_START_DATE = "startDate";


    String PARAM_END_DATE = "endDate";

}
