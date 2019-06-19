package com.zhouq.sparkproject.spark;

import com.alibaba.fastjson.JSONObject;
import com.zhouq.sparkproject.conf.ConfigurationManager;
import com.zhouq.sparkproject.constant.Constants;
import com.zhouq.sparkproject.dao.ITaskDao;
import com.zhouq.sparkproject.dao.impl.DAOFactory;
import com.zhouq.sparkproject.domain.Task;
import com.zhouq.sparkproject.test.MockData;
import com.zhouq.sparkproject.util.ParamUtils;
import com.zhouq.sparkproject.util.StringUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.hive.HiveContext;
import scala.Tuple2;

import java.util.Iterator;

/**
 * 用户访问session 分析Spark 作业
 * 接收用户创建的分析任务，用户可能指定的条件如下：
 * 1、时间范围：起始日期~结束日期
 * 2、性别：男和女
 * 3、年龄范围
 * 4、职业：多选
 * 5、城市：多选
 * 6、搜索词：多个搜索词，只要某个session 中的任何一个action 搜索过一个指定的关键词，那么这个session 就符合条件
 * 7、点击品类：多个商品，只要某个session 中的任何一个action 点击过某个品类，那么这个session 就符合条件。
 * <p>
 * 我们spark作业如何接受用户创建的任务？
 * J2EE 平台接受用户创建的任务请求后，就会将任务信息插入MYSQL的task 表中。任务参数以JSON 的格式存储在task_param 字段中。
 * 接着J2EE 平台会执行我们的spark-submit shell 脚本，并将taskid 作为参数传递给spark-submit shell 脚本，
 * spark-submit shell 脚本，在执行时，是可以接受参数的，并且会将接收的参数传递给Spark 作业的main 函数，
 * 参数就封装在main 函数的args数组中。
 * <p>
 * 这是spark 提供的特性
 */
public class UserVisitSessionAnalyzeSpark {
    public static void main(String[] args) {
        //构建 Spark 上下文
        SparkConf conf = new SparkConf().setAppName(Constants.SPARK_APP_NAME_SESSION).setMaster("local");

        JavaSparkContext sc = new JavaSparkContext(conf);
        SQLContext sqlContext = getSQLContext(sc.sc());

        //生产模拟测试数据
        mockData(sc, sqlContext);

        //创建需要使用的DAO 组件
        ITaskDao taskDao = DAOFactory.getTaskDao();

        //如果要进行 session 粒度的数据聚合
        // 首先从user_visit_action 表中，查询出来指定日期范围内的行为数据。
        // 如果要根据用户在创建人物时指定的参数，来进行数据过滤和筛选。

        Long taskId = ParamUtils.getTaskIdFromArgs(args);
        Task task = taskDao.findById(taskId);

        //获取到任务参数
        JSONObject parseObject = JSONObject.parseObject(task.getTaskParam());


        //关闭Spark 上下文
        sc.close();
    }


    /**
     * 获取 SQLContext
     * 如果是本地模式，则创建一个 SQLContext
     * 如果是生产环境，那么就创建一个HiveContext 对象。
     *
     * @param sc
     * @return
     */
    private static SQLContext getSQLContext(SparkContext sc) {
        boolean local = ConfigurationManager.getBoolean(Constants.SPARK_LOCAL);
        if (local) {
            return new SQLContext(sc);
        } else {
            return new HiveContext(sc);
        }
    }

    /**
     * 生成模拟数据，只在本地模式生效
     *
     * @param sc
     * @param sqlContext
     */
    private static void mockData(JavaSparkContext sc, SQLContext sqlContext) {
        boolean local = ConfigurationManager.getBoolean(Constants.SPARK_LOCAL);
        if (local) {
            MockData.mock(sc, sqlContext);
        }
    }

    /**
     * 查询时间段内的用户行为数据
     *
     * @param sqlContext
     * @param taskParam
     * @return 行为数据的 RDD
     */
    private static JavaRDD<Row> getActionRDDByDateRange(SQLContext sqlContext, JSONObject taskParam) {
        String startDate = ParamUtils.getParam(taskParam, Constants.PARAM_START_DATE);
        String endDate = ParamUtils.getParam(taskParam, Constants.PARAM_END_DATE);

        String sql = "select * " +
                "from user_visit_action" +
                "where date >='" + startDate + "'" +
                "and date <='" + endDate + "'";

        DataFrame actionDF = sqlContext.sql(sql);

        return actionDF.javaRDD();
    }


    /**
     * 对行为数据按 session 粒度进行聚合
     *
     * @param actionRDD 行为数据的RDD
     * @return session 粒度聚合数据
     */
    private static JavaPairRDD<String, String> aggregateBySession(JavaRDD<Row> actionRDD) {

        // 现在 actionRDD 中的元素就是ROW，一个ROW 就是一行用户范文记录。比如一次点击或者搜素。
        // 我们现在需要将这个Row 映射成 <sessionId,Row> 的格式

        JavaPairRDD<String, Row> sessionid2ActionRDD = actionRDD.mapToPair(
                /**
                 * PairFunction
                 * 第一个参数，相当于函数的输入
                 * 第二个参数和第三个参数，相当于函数的输出（Tuple），分别是Tuple 的第一个和第二个值
                 */
                new PairFunction<Row, String, Row>() {
                    @Override
                    public Tuple2<String, Row> call(Row row) throws Exception {
                        //sessionid 在第3个位置
                        return new Tuple2<>(row.getString(2), row);
                    }
                });

        // 对行为数据按session 粒度进行分组
        JavaPairRDD<String, Iterable<Row>> sessionid2ActionsRDD = sessionid2ActionRDD.groupByKey();

        // 对每一个 session 分组进行聚合，将session 中所有的搜索词和点击品类都聚合起来。
        JavaPairRDD<String, String> session2PartAggrInfoRDD = sessionid2ActionsRDD.mapToPair(
                new PairFunction<Tuple2<String, Iterable<Row>>, String, String>() {
                    @Override
                    public Tuple2<String, String> call(Tuple2<String, Iterable<Row>> tuple) throws Exception {
                        String sessionid =tuple._1;
                        Iterator<Row> iterator = tuple._2.iterator();

                        StringBuffer searchKeywordsBuffer = new StringBuffer();
                        StringBuffer clickCategoryIdsBuffer = new StringBuffer();

                        while (iterator.hasNext()){
                            Row row = iterator.next();

                            String searchKeyWord = row.getString(5);
                            Long clickCategoryId = row.getLong(6);

                            // 实际数据中不可能都包含这些搜索词语和 点击品类
                            // 搜索行为才会有搜索关键字 searchKeyword
                            // 点击品类的行为才有 clickCategoryId
                            // 所以，任何一个行为数据是不可能有两个字段的。所以数据是可能出现 null  的情况。

                            //我们决定是否要将搜索词跟品类id 拼接到字符串中去
                            // 首先要满足：不是null  值。
                            // 其次，之前的字符串中还有没有搜索关键词或者点击品类id ,没有再拼接。

                            if (StringUtils.isNotEmpty(searchKeyWord)){
                                if (!searchKeywordsBuffer.toString().contains(searchKeyWord)){
                                    searchKeywordsBuffer.append(searchKeyWord+",");
                                }
                            }

                            if (clickCategoryId != null){
                                if (!clickCategoryIdsBuffer.toString().contains(String.valueOf(clickCategoryId))){
                                    clickCategoryIdsBuffer.append(clickCategoryId +",");
                                }
                            }

                            String searchKeywords = StringUtils.trimComma(searchKeywordsBuffer.toString());
                            String clickGategoryIds = StringUtils.trimComma(clickCategoryIdsBuffer.toString());




                        }

                        return null;
                    }
        });



        return null;
    }
}
