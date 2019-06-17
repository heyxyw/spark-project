package com.zhouq.sparkproject.jdbc;

import com.zhouq.sparkproject.conf.ConfigurationManager;
import com.zhouq.sparkproject.constant.Constants;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.LinkedList;
import java.util.List;

/**
 * JDBC 辅助组件
 *
 * @Author: zhouq
 * @Date: 2019-06-16
 */
@SuppressWarnings("Duplicates")
public class JDBCHelper {

    /**
     * 第一步，在静态代码块中直接加载数据库驱动
     */
    static {
        try {
            String driver = ConfigurationManager.getProperty(Constants.JDBC_DRIVER);
            Class.forName(driver);
        }catch (Exception e){
            e.printStackTrace();
        }
    }

    private static JDBCHelper instance = null;

    /**
     * 数据库连接池
     */
    private LinkedList<Connection> datasource = null;

    /**
     * 第二步，私有化构造方法
     *
     * JDBCHelper 在整个程序运行生命周期中，只会创建一次实例。
     * 在这一次创建实例的过程中，就会调用 JDBCHelper() 构造方法
     * 此时，就可以在构造方法中，去创建自己唯一的一个数据库连接池
     *
     */
    private JDBCHelper(){
        //先获取数据库连接池的大小
        Integer datasourceSize = ConfigurationManager.getInteger(Constants.JDBC_DATASOURCE_SIZE);

        //然后创建指定数量的数据库连接，然后放进数据库连接池中

        String url = ConfigurationManager.getProperty(Constants.JDBC_URL);
        String user = ConfigurationManager.getProperty(Constants.JDBC_USER);
        String password = ConfigurationManager.getProperty(Constants.JDBC_PASSWORD);

        datasource = new LinkedList<>();
        for (int i = 0;i < datasourceSize ; i++){
            try {
                Connection conn = DriverManager.getConnection(url, user, password);
                datasource.push(conn);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    /**
     * 第三步，实现单利
     * @return
     */
    public static JDBCHelper getInstance(){
        if (instance == null){
            synchronized (JDBCHelper.class){
                if (instance ==  null){
                    instance = new JDBCHelper();
                }
            }
        }
        return instance;
    }

    /**
     * 第四步，提供获取数据库连接的方法
     * 有可能你去获取的时候，这个时候，连接都被用光来。你暂时获取不到数据库连接。
     * 我们实现一个简单的等待机制，去等待获取到数据库连接。
     */
    public synchronized Connection getConnection(){
        while (datasource.size() == 0){
            try {
                Thread.sleep(10);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
        return datasource.poll();
    }

    /**
     * 第五步：开发CRUD 的方法
     * 1.执行增删改SQL 语句的方法
     * 2.执行查询SQL 语句的方法
     * 3.执行批量插入
     */

    /**
     * 执行增删改 SQL 语句
     * @param sql
     * @param params
     * @return 影响的行数
     */
    public int executeUpdate(String sql,Object[] params){
        int rtn = 0;
        Connection conn = null;
        PreparedStatement pstmt = null;

        try {
            conn = getConnection();
            pstmt = conn.prepareStatement(sql);

            for (int i = 0; i< params.length ; i++){
                pstmt.setObject( i+ 1,params[i]);
            }

            rtn = pstmt.executeUpdate();
        }catch (Exception e){
            e.printStackTrace();
        }finally {
            if (conn != null){
                datasource.push(conn);
            }
        }

        return rtn;
    }


    /**
     * 执行查询SQL语句
     * @param sql
     * @param params
     * @param callback
     */
    public void executeQuery(String sql,Object[] params,QueryCallback callback){
        Connection conn = null;
        PreparedStatement pstmt = null;
        ResultSet rs = null;

        try{

            conn = getConnection();
            pstmt = conn.prepareStatement(sql);

            for(int i = 0; i < params.length ;i ++){
                pstmt.setObject(i + 1,params[i]);
            }

            rs =pstmt.executeQuery();

            callback.process(rs);
        }catch (Exception e){
            e.printStackTrace();
        }finally {
            if (conn != null){
                datasource.push(conn);
            }
        }
    }

    /**
     * 批量执行SQL 语句
     * @param sql
     * @param paramsList
     * @return 每条语句影响的行数
     */
    public int[] executeBatch(String sql, List<Object[]> paramsList){
        Connection conn = null;
        PreparedStatement pstmt = null;

        int[] rtn = null;
        try {

            conn = getConnection();

            //第一步，使用 Connection 取消自动提交
            conn.setAutoCommit(false);
            pstmt = conn.prepareStatement(sql);

            //第二步，封装参数
            for (Object[] params : paramsList){
                for (int i =0 ;i < params.length ; i++){
                    pstmt.setObject(i +1,params[i]);
                }
                pstmt.addBatch();
            }
            //第三步，使用 executeBatch 执行批量SQL
            rtn = pstmt.executeBatch();

            //最后提交批量的SQL 语句，再设置连接为自动提交
            conn.commit();
            conn.setAutoCommit(true);
        }catch (Exception e){
            e.printStackTrace();
        }
        finally {
            if (conn !=null){
                datasource.push(conn);
            }
        }
        return rtn;
    }


    /**
     * 静态内部类： 查询回调接口
     */
    public static interface QueryCallback {
        void process(ResultSet rs) throws Exception;
    }

}
