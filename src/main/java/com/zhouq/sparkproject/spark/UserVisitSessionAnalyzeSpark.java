package com.zhouq.sparkproject.spark;

import com.alibaba.fastjson.JSONObject;
import com.zhouq.sparkproject.conf.ConfigurationManager;
import com.zhouq.sparkproject.constant.Constants;
import com.zhouq.sparkproject.dao.ITaskDao;
import com.zhouq.sparkproject.dao.impl.DAOFactory;
import com.zhouq.sparkproject.domain.Task;
import com.zhouq.sparkproject.test.MockData;
import com.zhouq.sparkproject.util.DateUtils;
import com.zhouq.sparkproject.util.ParamUtils;
import com.zhouq.sparkproject.util.StringUtils;
import com.zhouq.sparkproject.util.ValidUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.hive.HiveContext;
import scala.Tuple2;

import java.util.Date;
import java.util.Iterator;
import java.util.List;

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

        args = new String[]{"1"};

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

        // 如果要进行session粒度的数据聚合
        // 首先要从user_visit_action表中，查询出来指定日期范围内的行为数据
        JavaRDD<Row> actionRDD = getActionRDDByDateRange(sqlContext, parseObject);

        // 首先，可以将行为数据，按照session_id进行groupByKey分组
        // 此时的数据的粒度就是session粒度了，然后呢，可以将session粒度的数据
        // 与用户信息数据，进行join
        // 然后就可以获取到session粒度的数据，同时呢，数据里面还包含了session对应的user的信息
        // 到现在为止，获取得数据为 <sessionid,(sessionid,searchkeyword,clickCategory,userid,username,xxxx)>
        JavaPairRDD<String, String> sessionid2AggrInfoRDD = aggregateBySession(actionRDD, sqlContext);

        System.out.println(sessionid2AggrInfoRDD.count());

        List<Tuple2<String, String>> tuple2List = sessionid2AggrInfoRDD.take(10);
        for (Tuple2<String, String> tuple2 : tuple2List) {
            System.out.println(tuple2);
        }

        // 接着，就要针对session粒度的聚合数据，按照使用者指定的筛选参数进行数据过滤
        // 相当于我们自己编写的算子，是要访问外面的任务参数对象的
        // 所以，大家记得我们之前说的，匿名内部类（算子函数），访问外部对象，是要给外部对象使用final修饰的
        JavaPairRDD<String, String> filteredSessionid2AggrInfoRDD = filterSession(sessionid2AggrInfoRDD, parseObject);


        /**
         *  session聚合统计（统计出访问时长和访问步长，各个区间的session数量占总session数量的比例）
         *
         * 如果不进行重构，直接来实现，思路：
         * 1、actionRDD，映射成<sessionid,Row>的格式
         * 2、按sessionid聚合，计算出每个session的访问时长和访问步长，生成一个新的RDD
         * 3、遍历新生成的RDD，将每个session的访问时长和访问步长，去更新自定义Accumulator中的对应的值
         * 4、使用自定义Accumulator中的统计值，去计算各个区间的比例
         * 5、将最后计算出来的结果，写入MySQL对应的表中
         *
         * 普通实现思路的问题：
         * 1、为什么还要用actionRDD，去映射？其实我们之前在session聚合的时候，映射已经做过了。多此一举
         * 2、是不是一定要，为了session的聚合这个功能，单独去遍历一遍session？其实没有必要，已经有session数据
         * 		之前过滤session的时候，其实，就相当于，是在遍历session，那么这里就没有必要再过滤一遍了
         *
         * 重构实现思路：
         * 1、不要去生成任何新的RDD（处理上亿的数据）
         * 2、不要去单独遍历一遍session的数据（处理上千万的数据）
         * 3、可以在进行session聚合的时候，就直接计算出来每个session的访问时长和访问步长
         * 4、在进行过滤的时候，本来就要遍历所有的聚合session信息，此时，就可以在某个session通过筛选条件后
         * 		将其访问时长和访问步长，累加到自定义的Accumulator上面去
         * 5、就是两种截然不同的思考方式，和实现方式，在面对上亿，上千万数据的时候，甚至可以节省时间长达
         * 		半个小时，或者数个小时
         *
         * 开发Spark大型复杂项目的一些经验准则：
         * 1、尽量少生成RDD
         * 2、尽量少对RDD进行算子操作，如果有可能，尽量在一个算子里面，实现多个需要做的功能
         * 3、尽量少对RDD进行shuffle算子操作，比如groupByKey、reduceByKey、sortByKey（map、mapToPair）
         * 		shuffle操作，会导致大量的磁盘读写，严重降低性能
         * 		有shuffle的算子，和没有shuffle的算子，甚至性能，会达到几十分钟，甚至数个小时的差别
         * 		有shfufle的算子，很容易导致数据倾斜，一旦数据倾斜，简直就是性能杀手（完整的解决方案）
         * 4、无论做什么功能，性能第一
         * 		在传统的J2EE或者.NET后者PHP，软件/系统/网站开发中，我认为是架构和可维护性，可扩展性的重要
         * 		程度，远远高于了性能，大量的分布式的架构，设计模式，代码的划分，类的划分（高并发网站除外）
         *
         * 		在大数据项目中，比如MapReduce、Hive、Spark、Storm，我认为性能的重要程度，远远大于一些代码
         * 		的规范，和设计模式，代码的划分，类的划分；大数据，大数据，最重要的，就是性能
         * 		主要就是因为大数据以及大数据项目的特点，决定了，大数据的程序和项目的速度，都比较慢
         * 		如果不优先考虑性能的话，会导致一个大数据处理程序运行时间长度数个小时，甚至数十个小时
         * 		此时，对于用户体验，简直就是一场灾难
         *
         * 		所以，推荐大数据项目，在开发和代码的架构中，优先考虑性能；其次考虑功能代码的划分、解耦合
         *
         * 		我们如果采用第一种实现方案，那么其实就是代码划分（解耦合、可维护）优先，设计优先
         * 		如果采用第二种方案，那么其实就是性能优先
         *
         * 		讲了这么多，其实大家不要以为我是在岔开话题，大家不要觉得项目的课程，就是单纯的项目本身以及
         * 		代码coding最重要，其实项目，我觉得，最重要的，除了技术本身和项目经验以外；非常重要的一点，就是
         * 		积累了，处理各种问题的经验
         *
         */


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
                " where date >='" + startDate + "'" +
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
    private static JavaPairRDD<String, String> aggregateBySession(JavaRDD<Row> actionRDD, SQLContext sqlContext) {

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
        JavaPairRDD<Long, String> userid2PartAggrInfoRDD = sessionid2ActionsRDD.mapToPair(
                new PairFunction<Tuple2<String, Iterable<Row>>, Long, String>() {
                    @Override
                    public Tuple2<Long, String> call(Tuple2<String, Iterable<Row>> tuple) throws Exception {
                        String sessionid = tuple._1;
                        Iterator<Row> iterator = tuple._2.iterator();

                        StringBuffer searchKeywordsBuffer = new StringBuffer();
                        StringBuffer clickCategoryIdsBuffer = new StringBuffer();

                        Long userId = null;

                        // session的起始和结束时间
                        Date startTime = null;
                        Date endTime = null;
                        // session的访问步长
                        int stepLength = 0;

                        while (iterator.hasNext()) {
                            Row row = iterator.next();

                            if (userId == null) {
                                userId = row.getLong(1);
                            }

                            String searchKeyWord = row.getString(5);
                            Long clickCategoryId = row.getLong(6);

                            // 实际数据中不可能都包含这些搜索词语和 点击品类
                            // 搜索行为才会有搜索关键字 searchKeyword
                            // 点击品类的行为才有 clickCategoryId
                            // 所以，任何一个行为数据是不可能有两个字段的。所以数据是可能出现 null  的情况。

                            //我们决定是否要将搜索词跟品类id 拼接到字符串中去
                            // 首先要满足：不是null  值。
                            // 其次，之前的字符串中还有没有搜索关键词或者点击品类id ,没有再拼接。

                            if (StringUtils.isNotEmpty(searchKeyWord)) {
                                if (!searchKeywordsBuffer.toString().contains(searchKeyWord)) {
                                    searchKeywordsBuffer.append(searchKeyWord + ",");
                                }
                            }

                            if (clickCategoryId != null) {
                                if (!clickCategoryIdsBuffer.toString().contains(String.valueOf(clickCategoryId))) {
                                    clickCategoryIdsBuffer.append(clickCategoryId + ",");
                                }
                            }

                            // 开始计算 session 的开始时间和结束时间
                            Date actionTime = DateUtils.parseTime(row.getString(4));

                            if (startTime == null) {
                                startTime = actionTime;
                            }
                            if (endTime == null) {
                                endTime = actionTime;
                            }

                            if (actionTime.before(startTime)) {
                                startTime = actionTime;
                            }
                            if (actionTime.after(endTime)) {
                                endTime = actionTime;
                            }

                            stepLength++;
                        }

                        String searchKeywords = StringUtils.trimComma(searchKeywordsBuffer.toString());
                        String clickGategoryIds = StringUtils.trimComma(clickCategoryIdsBuffer.toString());

                        //计算session访问时长
                        long visitLength = (endTime.getTime() - startTime.getTime()) / 1000;

                        /**
                         * 思考？
                         * 我们返回的数据格式，即是<sessionid,partAggrInfo>
                         * 但是，我们这一步聚合完成以后，还是需要将每一行数据跟对应的用户信息进行聚合
                         * 那么，问题就来来。如果跟用户信息进行聚合，那么key 就不应该是sessionID
                         * 就应该是userID ,才能够跟<userid,Row> 格式的数据进行聚合
                         * 如果我们这里直接返回 <sessionid,partAggrInfo> 还得进行一次maptopair 算子
                         * 将RDD 映射成 <userid，partAggrInfo> 的格式，那么就多此一举咯。
                         *
                         * 所以，我们这里可以直接返回的数据格式就是<userId,partAggrInfo>
                         * 然后跟用户信息进行join的时候，将partAggrInfo 关联上userInfo
                         * 然后再直接将返回的tuple 的key 设置成sessionID
                         *
                         * 那么最后的数据格式还是 <userID,fullAggrInfo>
                         *
                         * 聚合数据的时候，我们统一使用key=value|key=value 的方式进行拼接数据
                         */


                        String partAggrInfo = Constants.FIELD_SESSION_ID + "=" + sessionid + "|"
                                + Constants.FIELD_SEARCH_KEYWORDS + "=" + searchKeywords + "|"
                                + Constants.FIELD_CLICK_CATEGORY_IDS + "=" + clickGategoryIds + "|"
                                + Constants.FIELD_VISIT_LENGTH + "=" + visitLength + "|"
                                + Constants.FIELD_STEP_LENGTH + "=" + stepLength;

                        return new Tuple2<Long, String>(userId, partAggrInfo);
                    }
                });

        //查询所有用户的数据，并映射成<userId,Row> 的格式
        String sql = "select * from user_info";

        JavaRDD<Row> userInfoRDD = sqlContext.sql(sql).toJavaRDD();

        JavaPairRDD<Long, Row> userid2InfoRDD = userInfoRDD.mapToPair(new PairFunction<Row, Long, Row>() {
            @Override
            public Tuple2<Long, Row> call(Row row) throws Exception {
                return new Tuple2<>(row.getLong(0), row);
            }
        });

        // 将session粒度聚合数据，与用户信息进行join, join 以后的数据为 <userid,<partAggrInfo,userRow>>
        JavaPairRDD<Long, Tuple2<String, Row>> userid2FullInfoRDD = userid2PartAggrInfoRDD.join(userid2InfoRDD);

        // 对join起来的数据进行拼接，并且返回<sessionid,fullAggrInfo>格式的数据
        JavaPairRDD<String, String> sessionid2FullAggrInfoRDD = userid2FullInfoRDD.mapToPair(
                new PairFunction<Tuple2<Long, Tuple2<String, Row>>, String, String>() {

                    @Override
                    public Tuple2<String, String> call(Tuple2<Long, Tuple2<String, Row>> tuple) throws Exception {
                        String partAggrInfo = tuple._2._1;
                        Row userInfoRow = tuple._2._2;

                        String sessionId = StringUtils.getFieldFromConcatString(partAggrInfo, "\\|", Constants.FIELD_SESSION_ID);

                        //解析 userinfo 拼接上去
                        int age = userInfoRow.getInt(3);
                        String professional = userInfoRow.getString(4);
                        String city = userInfoRow.getString(5);
                        String sex = userInfoRow.getString(6);

                        String fullAggrInfo = partAggrInfo + "|"
                                + Constants.FIELD_AGE + "=" + age + "|"
                                + Constants.FIELD_PROFESSIONAL + "=" + professional + "|"
                                + Constants.FIELD_CITY + "=" + city + "|"
                                + Constants.FIELD_SEX + "=" + sex;

                        return new Tuple2<>(sessionId, fullAggrInfo);
                    }
                });

        return sessionid2FullAggrInfoRDD;
    }

    /**
     * 过滤 session 数据
     *
     * @param sessionid2AggrInfoRDD
     * @param params
     * @return
     */
    private static JavaPairRDD<String, String> filterSession(
            JavaPairRDD<String, String> sessionid2AggrInfoRDD, final JSONObject taskParam) {

        // 为了使用我们后面的ValieUtils，所以，首先将所有的筛选参数拼接成一个连接串
        String startAge = ParamUtils.getParam(taskParam, Constants.PARAM_START_AGE);
        String endAge = ParamUtils.getParam(taskParam, Constants.PARAM_END_AGE);
        String professionals = ParamUtils.getParam(taskParam, Constants.PARAM_PROFESSIONALS);
        String cities = ParamUtils.getParam(taskParam, Constants.PARAM_CITIES);
        String sex = ParamUtils.getParam(taskParam, Constants.PARAM_SEX);
        String keywords = ParamUtils.getParam(taskParam, Constants.PARAM_KEYWORDS);
        String categoryIds = ParamUtils.getParam(taskParam, Constants.PARAM_CATEGORY_IDS);


        String _parameter = (startAge != null ? Constants.PARAM_START_AGE + "=" + startAge + "|" : "")
                + (endAge != null ? Constants.PARAM_END_AGE + "=" + endAge + "|" : "")
                + (professionals != null ? Constants.PARAM_PROFESSIONALS + "=" + professionals + "|" : "")
                + (cities != null ? Constants.PARAM_CITIES + "=" + cities + "|" : "")
                + (sex != null ? Constants.PARAM_SEX + "=" + sex + "|" : "")
                + (keywords != null ? Constants.PARAM_KEYWORDS + "=" + keywords + "|" : "")
                + (categoryIds != null ? Constants.PARAM_CATEGORY_IDS + "=" + categoryIds : "");

        if (_parameter.endsWith("\\|")) {
            _parameter = _parameter.substring(0, _parameter.length() - 1);
        }

        final String parameter = _parameter;

        //根据筛选参数进行过滤
        JavaPairRDD<String, String> fliteredSessionid2AggrInfoRDD = sessionid2AggrInfoRDD.filter(
                new Function<Tuple2<String, String>, Boolean>() {
                    @Override
                    public Boolean call(Tuple2<String, String> tuple) throws Exception {
                        //从 tuple 取出 聚合数据

                        String aggrInfo = tuple._2;
                        //先按照年龄筛选进行过滤，
                        //范围在（startAge,endAge） 之间的
                        if (!ValidUtils.between(aggrInfo, Constants.FIELD_AGE,
                                parameter, Constants.PARAM_START_AGE, Constants.PARAM_END_AGE)) {
                            return false;
                        }

                        // 按照职业范围进行过滤（professionals）
                        // 互联网,IT,软件
                        // 互联网
                        if (!ValidUtils.in(aggrInfo, Constants.FIELD_PROFESSIONAL, parameter, Constants.PARAM_PROFESSIONALS)) {
                            return false;
                        }

                        // 按照城市范围进行过滤（cities）
                        // 北京,上海,广州,深圳
                        // 成都

                        if (!ValidUtils.in(aggrInfo, Constants.FIELD_CITY, parameter, Constants.PARAM_CITIES)) {
                            return false;
                        }

                        // 按照性别进行过滤
                        // 男 或者女 完全匹配

                        if (!ValidUtils.equal(aggrInfo, Constants.FIELD_SEX, parameter, Constants.PARAM_SEX)) {
                            return false;
                        }

                        // 按照搜索词进行过滤
                        // 我们的session可能搜索了 火锅,蛋糕,烧烤
                        // 我们的筛选条件可能是 火锅,串串香,iphone手机
                        // 那么，in这个校验方法，主要判定session搜索的词中，有任何一个，与筛选条件中
                        // 任何一个搜索词相当，即通过

                        if (!ValidUtils.in(aggrInfo, Constants.FIELD_SEARCH_KEYWORDS, parameter, Constants.PARAM_KEYWORDS)) {
                            return false;
                        }

                        // 按照点击品类id进行过滤
                        if (!ValidUtils.in(aggrInfo, Constants.FIELD_CLICK_CATEGORY_IDS, parameter, Constants.PARAM_CATEGORY_IDS)) {
                            return false;
                        }

                        return true;
                    }
                });

        return fliteredSessionid2AggrInfoRDD;
    }
}
