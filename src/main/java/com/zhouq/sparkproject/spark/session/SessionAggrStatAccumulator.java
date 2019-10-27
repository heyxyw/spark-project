package com.zhouq.sparkproject.spark.session;

import com.zhouq.sparkproject.constant.Constants;
import com.zhouq.sparkproject.util.StringUtils;
import org.apache.spark.AccumulatorParam;

/**
 * Create by zhouq on 2019/6/26
 */
public class SessionAggrStatAccumulator implements AccumulatorParam<String> {

    /**
     * zero方法，其实主要用于数据的初始化
     * 那么，我们这里，就返回一个值，就是初始化中，所有范围区间的数量，都是0
     * 各个范围区间的统计数量的拼接，还是采用一如既往的 key=value|key=value 的连接串的格式
     */
    @Override
    public String zero(String initialValue) {

        return Constants.SESSION_COUNT + "=0|"
                + Constants.TIME_PERIOD_1s_3s + "=0|"
                + Constants.TIME_PERIOD_4s_6s + "=0|"
                + Constants.TIME_PERIOD_7s_9s + "=0|"
                + Constants.TIME_PERIOD_10s_30s + "=0|"
                + Constants.TIME_PERIOD_30s_60s + "=0|"
                + Constants.TIME_PERIOD_1m_3m + "=0|"
                + Constants.TIME_PERIOD_3m_10m + "=0|"
                + Constants.TIME_PERIOD_10m_30m + "=0|"
                + Constants.TIME_PERIOD_30m + "=0|"
                + Constants.STEP_PERIOD_1_3 + "=0|"
                + Constants.STEP_PERIOD_4_6 + "=0|"
                + Constants.STEP_PERIOD_7_9 + "=0|"
                + Constants.STEP_PERIOD_10_30 + "=0|"
                + Constants.STEP_PERIOD_30_60 + "=0|"
                + Constants.STEP_PERIOD_60 + "=0";
    }

    @Override
    public String addAccumulator(String v1, String v2) {
        return add(v1, v2);
    }

    /**
     * addInPlace 和 addAccumulator 可以理解为一样的。
     * 这两个方法，v1 就是我们初始化的那个连接字符串
     * 然后 v2 就是我们遍历 session 的时候，判断出某个 session 对应的区间。然后会用  Constants.TIME_PERIOD_1s_3s
     * <p>
     * 所以我们需要做的事情就是 在V1 中，找到v2 对应的 value ，然后累加 1，然后再更新回连接串里面去
     *
     */
    @Override
    public String addInPlace(String v1, String v2) {
        return add(v1, v2);
    }

    private String add(String v1, String v2) {
        //校验： v1 为空的话，直接返回v2
        if (StringUtils.isEmpty(v1)) {
            return v2;
        }

        // 使用StringUtils工具类，从v1中，提取v2对应的值，并累加1

        String oldValue = StringUtils.getFieldFromConcatString(v1, "\\|", v2);

        if (oldValue != null) {
            int newValue = Integer.valueOf(oldValue) + 1;
            return StringUtils.setFieldInConcatString(v1, "\\|", v2, String.valueOf(newValue));
        }

        return v1;

    }

}
