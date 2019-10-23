package com.zhouq.sparkproject.spark;

import com.alibaba.fastjson.JSONObject;
import com.zhouq.sparkproject.conf.ConfigurationManager;
import com.zhouq.sparkproject.constant.Constants;
import com.zhouq.sparkproject.dao.ISessionAggrStatDAO;
import com.zhouq.sparkproject.dao.ITaskDao;
import com.zhouq.sparkproject.dao.impl.DAOFactory;
import com.zhouq.sparkproject.domain.SessionAggrStat;
import com.zhouq.sparkproject.domain.Task;
import com.zhouq.sparkproject.test.MockData;
import com.zhouq.sparkproject.util.DateUtils;
import com.zhouq.sparkproject.util.ParamUtils;
import com.zhouq.sparkproject.util.StringUtils;
import com.zhouq.sparkproject.util.ValidUtils;
import com.zhouq.sparkproject.util.NumberUtils;
import org.apache.spark.Accumulator;
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

import java.util.*;

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

//        System.out.println(sessionid2AggrInfoRDD.count());
//
//        List<Tuple2<String, String>> tuple2List = sessionid2AggrInfoRDD.take(10);
//        for (Tuple2<String, String> tuple2 : tuple2List) {
//            System.out.println(tuple2);
//        }

        // 接着，就要针对session粒度的聚合数据，按照使用者指定的筛选参数进行数据过滤
        // 相当于我们自己编写的算子，是要访问外面的任务参数对象的
        // 所以，大家记得我们之前说的，匿名内部类（算子函数），访问外部对象，是要给外部对象使用final修饰的


        // 重构，同时进行过滤和统计
        Accumulator<String> sessionAggrStatAccumulator = sc.accumulator("", new SessionAggrStatAccumulator());

        JavaPairRDD<String, String> filteredSessionid2AggrInfoRDD = filterSession(sessionid2AggrInfoRDD, parseObject, sessionAggrStatAccumulator);

        /**
         * 重点：对于Accumulator这种分布式累加计算的变量的使用，有一个重要说明
         *
         * 在 Accumulator 中，获取数据必须是在某一个 action 之后再进行的。
         * 如果没有action的话，那么程序根本就不会运行的。
         *
         * 是不是在calculateAndPersisitAggrStat方法之后，运行一个action操作，比如count、take
         * 不对！！！ 他们只是一个 transform 操作，并不是一个 action 操作。
         *
         * 必须把能够触发job执行的操作，放在最终写入MySQL方法之前
         *
         * 计算出来的结果，在J2EE中，是怎么显示的，是用两张柱状图显示
         */

        System.out.println(filteredSessionid2AggrInfoRDD.count());


        /**
         * 特别说明：我们知道，要将上一个功能的session 聚合统计的数据获取到。就必须是在一个action 操作
         * 触发job 之后才能从 Accumulator 中获取数据，否则是获取不到数据的，因为job 没有执行，Accumulator 的值
         * 为空，所以我们这里，将随机抽取session 的功能代码实现，放在session 聚合统计功能的最终计算和写库之前
         * 因为随机抽取功能中，有一个 countByKey 算子，是 action 操作，会触发 job 执行。
         */

        randomExtractSession(filteredSessionid2AggrInfoRDD);


        // 计算出各个范围的session占比，并写入MySQL
        calculateAndPersistAggrStat(sessionAggrStatAccumulator.value(), taskId);


        /**
         * 重点:
         *
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
                         * 重点
                         * 思考？
                         * 我们返回的数据格式，即是<sessionid,partAggrInfo>
                         * 但是，我们这一步聚合完成以后，还是需要将每一行数据跟对应的用户信息进行聚合
                         * 那么，问题就来了。如果跟用户信息进行聚合，那么key 就不应该是sessionID
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
                                + Constants.FIELD_STEP_LENGTH + "=" + stepLength + "|"
                                + Constants.FIELD_START_TIME + "=" + DateUtils.formatDate(startTime);

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
     * @param taskParam
     * @param sessionAggrStatAccumulator
     * @return
     */
    private static JavaPairRDD<String, String> filterSession(
            JavaPairRDD<String, String> sessionid2AggrInfoRDD, final JSONObject taskParam, final Accumulator<String> sessionAggrStatAccumulator) {

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

        if (_parameter.endsWith("|")) {
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

                        System.out.println(aggrInfo + "..." + parameter);
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


                        // 如果经过了之前的多个过滤条件之后，程序能够走到这里
                        // 那么就说明，该session是通过了用户指定的筛选条件的，也就是需要保留的session
                        // 那么就要对session的访问时长和访问步长，进行统计，根据session对应的范围
                        // 进行相应的累加计数

                        // 主要走到这一步，那么就是需要计数的session
                        sessionAggrStatAccumulator.add(Constants.SESSION_COUNT);

                        //计算出 session 的访问时长和访问步长的范围，并进行响应的累计

                        long visitLength = Long.valueOf(StringUtils.getFieldFromConcatString(aggrInfo, "\\|", Constants.FIELD_VISIT_LENGTH));
                        long stepLength = Long.valueOf(StringUtils.getFieldFromConcatString(aggrInfo, "\\|", Constants.FIELD_STEP_LENGTH));

                        calculateVisitLength(visitLength);
                        calculateStepLength(stepLength);

                        return true;
                    }

                    /**
                     * 计算访问时长范围
                     * @param visitLength
                     */
                    private void calculateVisitLength(long visitLength) {
                        if (visitLength >= 1 && visitLength <= 3) {
                            sessionAggrStatAccumulator.add(Constants.TIME_PERIOD_1s_3s);
                        } else if (visitLength >= 4 && visitLength <= 6) {
                            sessionAggrStatAccumulator.add(Constants.TIME_PERIOD_4s_6s);
                        } else if (visitLength >= 7 && visitLength <= 9) {
                            sessionAggrStatAccumulator.add(Constants.TIME_PERIOD_7s_9s);
                        } else if (visitLength >= 10 && visitLength <= 30) {
                            sessionAggrStatAccumulator.add(Constants.TIME_PERIOD_10s_30s);
                        } else if (visitLength > 30 && visitLength <= 60) {
                            sessionAggrStatAccumulator.add(Constants.TIME_PERIOD_30s_60s);
                        } else if (visitLength > 60 && visitLength <= 180) {
                            sessionAggrStatAccumulator.add(Constants.TIME_PERIOD_1m_3m);
                        } else if (visitLength > 180 && visitLength <= 600) {
                            sessionAggrStatAccumulator.add(Constants.TIME_PERIOD_3m_10m);
                        } else if (visitLength > 600 && visitLength <= 1800) {
                            sessionAggrStatAccumulator.add(Constants.TIME_PERIOD_10m_30m);
                        } else if (visitLength > 1800) {
                            sessionAggrStatAccumulator.add(Constants.TIME_PERIOD_30m);
                        }
                    }

                    /**
                     * 计算访问步长范围
                     * @param stepLength
                     */
                    private void calculateStepLength(long stepLength) {
                        if (stepLength >= 1 && stepLength <= 3) {
                            sessionAggrStatAccumulator.add(Constants.STEP_PERIOD_1_3);
                        } else if (stepLength >= 4 && stepLength <= 6) {
                            sessionAggrStatAccumulator.add(Constants.STEP_PERIOD_4_6);
                        } else if (stepLength >= 7 && stepLength <= 9) {
                            sessionAggrStatAccumulator.add(Constants.STEP_PERIOD_7_9);
                        } else if (stepLength >= 10 && stepLength <= 30) {
                            sessionAggrStatAccumulator.add(Constants.STEP_PERIOD_10_30);
                        } else if (stepLength > 30 && stepLength <= 60) {
                            sessionAggrStatAccumulator.add(Constants.STEP_PERIOD_30_60);
                        } else if (stepLength > 60) {
                            sessionAggrStatAccumulator.add(Constants.STEP_PERIOD_60);
                        }
                    }

                });

        return fliteredSessionid2AggrInfoRDD;
    }


    /**
     * 随机抽取 session
     *
     * @param sessionid2AggrInfoRDD
     */
    private static void randomExtractSession(JavaPairRDD<String, String> sessionid2AggrInfoRDD) {
        // 第一步，计算每天每小时的session 数量，获取<yyyy-MM-dd_HH,sessionid> 格式的数据
        JavaPairRDD<String, String> time2sessionidRDD = sessionid2AggrInfoRDD.mapToPair(new PairFunction<Tuple2<String, String>, String, String>() {
            @Override
            public Tuple2<String, String> call(Tuple2<String, String> tuple) throws Exception {
                String sessionid = tuple._1;
                String aggrInfo = tuple._2;
                String startTime = StringUtils.getFieldFromConcatString(aggrInfo, "\\|", Constants.FIELD_START_TIME);
                String dateHour = DateUtils.getDateHour(startTime);

                /** 注意点：思考一下 》？
                 *
                 * 由于我们后面还需要把抽取出来的session聚合 数据写入 session_random_extract 表。
                 * 所以这里我们第一个RDD 的value 就是不是sessionid 了，而是 session 的聚合数据
                 */
//                return new Tuple2<>(dateHour, sessionid);
                return new Tuple2<>(dateHour, aggrInfo);
            }
        });

        // 得到每天每个小时session 数量
        Map<String, Object> countMap = time2sessionidRDD.countByKey();

        // 第二步，使用按时间比例随机抽取算法，实现每天每小时要抽取的session 的索引

        // 将<yyyy-MM-dd_HH,count> 格式的map ,转换成 <yyyy-MM-dd,<HH,count>> 的格式
        Map<String, Map<String, Long>> dayHourCountMap = new HashMap<>();
        for (Map.Entry<String, Object> countEntry : countMap.entrySet()) {
            String dateHour = countEntry.getKey();
            String date = dateHour.split("_")[0];
            String hour = dateHour.split("_")[1];

            Long count = Long.valueOf(String.valueOf(countEntry.getValue()));

            Map<String, Long> hourCountMap = dayHourCountMap.get(date);
            if (hourCountMap == null) {
                hourCountMap = new HashMap<>();
                dayHourCountMap.put(date, hourCountMap);
            }

            hourCountMap.put(hour, count);
        }

        // 开始实现我们的按时间比例随机抽取算法

        // 总共要抽取 100个session，先按照天进行平分
        long extractNumbrePerDay = 100 / dayHourCountMap.size();

        //<date,<hour,(1,2,5,6)>>
        Map<String, Map<String, List<Integer>>> dateHourExtractMap = new HashMap<>();

        // 创建一个随机的 Random 对象。
        Random random = new Random();
        for (Map.Entry<String, Map<String, Long>> dateHourCountEntry : dayHourCountMap.entrySet()) {
            String date = dateHourCountEntry.getKey();
            Map<String, Long> hourCountMap = dateHourCountEntry.getValue();

            // 计算这一天的session 总数
            long sessionCount = 0L;
            for (Long hourCount : hourCountMap.values()) {
                sessionCount += hourCount;
            }

            Map<String, List<Integer>> hourExtractMap = dateHourExtractMap.get(date);
            if (hourExtractMap == null) {
                hourExtractMap = new HashMap<>();
                dateHourExtractMap.put(date, hourExtractMap);
            }

            // 遍历每个小时
            for (Map.Entry<String, Long> hourCountEntry : hourCountMap.entrySet()) {
                String hour = hourCountEntry.getKey();
                long hourCount = hourCountEntry.getValue();

                // 计算每个小时的 session 数量占据当天所有session 数量的比例，再直接乘以每天要抽取的session 数量，
                // 就可以计算出这个小时需要抽取的session 数量

                long hourExtractNumber = (long) ((double) hourCount / (double) sessionCount) * extractNumbrePerDay;

                if (hourExtractNumber > hourCount) {
                    hourExtractNumber = hourCount;
                }

                // 先获取当前小时存放随机数的list
                List<Integer> extractIndexList = hourExtractMap.get(hour);
                if (extractIndexList == null) {
                    extractIndexList = new ArrayList<>();
                    hourExtractMap.put(hour, extractIndexList);
                }

                // 生成上面计算出来的数量的随机数
                for (int i = 0; i < hourExtractNumber; i++) {
                    int extractIndex = random.nextInt((int) hourCount);
                    //索引不能重复
                    while (extractIndexList.contains(extractIndex)) {
                        extractIndex = random.nextInt((int) hourCount);
                    }
                    extractIndexList.add(extractIndex);
                }
            }
        }


    }

    /**
     * 计算各session 范围占比，并写入MySQL
     *
     * @param value
     * @param taskid
     */
    private static void calculateAndPersistAggrStat(String value, long taskid) {
        // 从Accumulator统计串中获取值

        long session_count = Long.valueOf(StringUtils.getFieldFromConcatString(value, "\\|", Constants.SESSION_COUNT));

        long visit_length_1s_3s = Long.valueOf(StringUtils.getFieldFromConcatString(value, "\\|", Constants.TIME_PERIOD_1s_3s));
        long visit_length_4s_6s = Long.valueOf(StringUtils.getFieldFromConcatString(value, "\\|", Constants.TIME_PERIOD_4s_6s));
        long visit_length_7s_9s = Long.valueOf(StringUtils.getFieldFromConcatString(value, "\\|", Constants.TIME_PERIOD_7s_9s));
        long visit_length_10s_30s = Long.valueOf(StringUtils.getFieldFromConcatString(value, "\\|", Constants.TIME_PERIOD_10s_30s));
        long visit_length_30s_60s = Long.valueOf(StringUtils.getFieldFromConcatString(value, "\\|", Constants.TIME_PERIOD_30s_60s));

        long visit_length_1m_3m = Long.valueOf(StringUtils.getFieldFromConcatString(value, "\\|", Constants.TIME_PERIOD_1m_3m));
        long visit_length_3m_10m = Long.valueOf(StringUtils.getFieldFromConcatString(value, "\\|", Constants.TIME_PERIOD_3m_10m));
        long visit_length_10m_30m = Long.valueOf(StringUtils.getFieldFromConcatString(value, "\\|", Constants.TIME_PERIOD_10m_30m));
        long visit_length_30m = Long.valueOf(StringUtils.getFieldFromConcatString(value, "\\|", Constants.TIME_PERIOD_30m));

        long step_length_1_3 = Long.valueOf(StringUtils.getFieldFromConcatString(value, "\\|", Constants.STEP_PERIOD_1_3));
        long step_length_4_6 = Long.valueOf(StringUtils.getFieldFromConcatString(value, "\\|", Constants.STEP_PERIOD_4_6));
        long step_length_7_9 = Long.valueOf(StringUtils.getFieldFromConcatString(value, "\\|", Constants.STEP_PERIOD_7_9));

        long step_length_10_30 = Long.valueOf(StringUtils.getFieldFromConcatString(value, "\\|", Constants.STEP_PERIOD_10_30));
        long step_length_30_60 = Long.valueOf(StringUtils.getFieldFromConcatString(value, "\\|", Constants.STEP_PERIOD_30_60));
        long step_length_60 = Long.valueOf(StringUtils.getFieldFromConcatString(value, "\\|", Constants.STEP_PERIOD_60));

        // 计算各个访问时长和访问步长的范围
        double visit_length_1s_3s_ratio = NumberUtils.formatDouble(
                (double) visit_length_1s_3s / (double) session_count, 2);
        double visit_length_4s_6s_ratio = NumberUtils.formatDouble(
                (double) visit_length_4s_6s / (double) session_count, 2);
        double visit_length_7s_9s_ratio = NumberUtils.formatDouble(
                (double) visit_length_7s_9s / (double) session_count, 2);
        double visit_length_10s_30s_ratio = NumberUtils.formatDouble(
                (double) visit_length_10s_30s / (double) session_count, 2);
        double visit_length_30s_60s_ratio = NumberUtils.formatDouble(
                (double) visit_length_30s_60s / (double) session_count, 2);
        double visit_length_1m_3m_ratio = NumberUtils.formatDouble(
                (double) visit_length_1m_3m / (double) session_count, 2);
        double visit_length_3m_10m_ratio = NumberUtils.formatDouble(
                (double) visit_length_3m_10m / (double) session_count, 2);
        double visit_length_10m_30m_ratio = NumberUtils.formatDouble(
                (double) visit_length_10m_30m / (double) session_count, 2);
        double visit_length_30m_ratio = NumberUtils.formatDouble(
                (double) visit_length_30m / (double) session_count, 2);

        double step_length_1_3_ratio = NumberUtils.formatDouble(
                (double) step_length_1_3 / (double) session_count, 2);
        double step_length_4_6_ratio = NumberUtils.formatDouble(
                (double) step_length_4_6 / (double) session_count, 2);
        double step_length_7_9_ratio = NumberUtils.formatDouble(
                (double) step_length_7_9 / (double) session_count, 2);
        double step_length_10_30_ratio = NumberUtils.formatDouble(
                (double) step_length_10_30 / (double) session_count, 2);
        double step_length_30_60_ratio = NumberUtils.formatDouble(
                (double) step_length_30_60 / (double) session_count, 2);
        double step_length_60_ratio = NumberUtils.formatDouble(
                (double) step_length_60 / (double) session_count, 2);

        // 将统计结果封装为Domain对象
        SessionAggrStat sessionAggrStat = new SessionAggrStat();
        sessionAggrStat.setTaskid(taskid);
        sessionAggrStat.setSession_count(session_count);
        sessionAggrStat.setVisit_length_1s_3s_ratio(visit_length_1s_3s_ratio);
        sessionAggrStat.setVisit_length_4s_6s_ratio(visit_length_4s_6s_ratio);
        sessionAggrStat.setVisit_length_7s_9s_ratio(visit_length_7s_9s_ratio);
        sessionAggrStat.setVisit_length_10s_30s_ratio(visit_length_10s_30s_ratio);
        sessionAggrStat.setVisit_length_30s_60s_ratio(visit_length_30s_60s_ratio);
        sessionAggrStat.setVisit_length_1m_3m_ratio(visit_length_1m_3m_ratio);
        sessionAggrStat.setVisit_length_3m_10m_ratio(visit_length_3m_10m_ratio);
        sessionAggrStat.setVisit_length_10m_30m_ratio(visit_length_10m_30m_ratio);
        sessionAggrStat.setVisit_length_30m_ratio(visit_length_30m_ratio);
        sessionAggrStat.setStep_length_1_3_ratio(step_length_1_3_ratio);
        sessionAggrStat.setStep_length_4_6_ratio(step_length_4_6_ratio);
        sessionAggrStat.setStep_length_7_9_ratio(step_length_7_9_ratio);
        sessionAggrStat.setStep_length_10_30_ratio(step_length_10_30_ratio);
        sessionAggrStat.setStep_length_30_60_ratio(step_length_30_60_ratio);
        sessionAggrStat.setStep_length_60_ratio(step_length_60_ratio);


        // 调用对应的DAO插入统计结果
        ISessionAggrStatDAO sessionAggrStatDAO = DAOFactory.getSessionAggrStatDAO();

        sessionAggrStatDAO.insert(sessionAggrStat);
    }
}
