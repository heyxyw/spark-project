package com.zhouq.sparkproject.spark;

import scala.math.Ordered;

/**
 * 品类二次排序
 * 封装你要进行排序算法需要的几个字段：点击次数、下单次数和支付次数
 * 然后实现 Ordered 接口
 * <p>
 * 根据其他的可以相比，如何来确定大于、大于等于、小于、小于等于
 * <p>
 * 然后依次使用三个数来进行比较，如果某一个相等，就比较下一个，依次类推。
 *
 * @Author: zhouq
 * @Date: 2019/10/24
 */
public class CategorySortKey implements Ordered<CategorySortKey> {
    private long clickCount;
    private long orderCount;
    private long payCount;


    @Override
    public int compare(CategorySortKey other) {
        if (clickCount - other.getClickCount() != 0) {
            return (int) (clickCount - other.getClickCount());
        } else if (orderCount - other.getOrderCount() != 0) {
            return (int) (orderCount - other.getOrderCount());
        } else if (payCount - other.getPayCount() != 0) {
            return (int) (payCount - other.getPayCount());
        }
        return 0;
    }

    /**
     * 小于 <
     *
     * @param other
     * @return
     */
    @Override
    public boolean $less(CategorySortKey other) {
        if (clickCount < other.getClickCount()) {
            return true;
        } else if (clickCount == other.getClickCount() &&
                orderCount < other.getOrderCount()) {
            return true;
        } else if (clickCount == other.getClickCount() &&
                orderCount == other.getOrderCount() &&
                payCount < other.getOrderCount()) {
            return true;
        }
        return false;
    }

    /**
     * 大于
     *
     * @param other
     * @return
     */
    @Override
    public boolean $greater(CategorySortKey other) {
        if (clickCount > other.getClickCount()) {
            return true;
        } else if (clickCount == other.getClickCount() &&
                orderCount > other.getOrderCount()) {
            return true;
        } else if (clickCount == other.getClickCount() &&
                orderCount == other.getOrderCount() &&
                payCount > other.getOrderCount()) {
            return true;
        }
        return false;
    }

    /**
     * 小于等于 <=
     *
     * @param other
     * @return
     */
    @Override
    public boolean $less$eq(CategorySortKey other) {
        if ($less(other)) {
            return true;
        } else if (clickCount == other.getClickCount() &&
                orderCount == other.getOrderCount() &&
                payCount > other.getOrderCount()) {
            return true;
        }
        return false;
    }

    /**
     * 大于等于 >=
     *
     * @param other
     * @return
     */
    @Override
    public boolean $greater$eq(CategorySortKey other) {
        if ($greater(other)) {
            return true;
        } else if (clickCount == other.getClickCount() &&
                orderCount == other.getOrderCount() &&
                payCount == other.getOrderCount()) {
            return true;
        }
        return false;
    }


    @Override
    public int compareTo(CategorySortKey other) {
        if (clickCount - other.getClickCount() != 0) {
            return (int) (clickCount - other.getClickCount());
        } else if (orderCount - other.getOrderCount() != 0) {
            return (int) (orderCount - other.getOrderCount());
        } else if (payCount - other.getPayCount() != 0) {
            return (int) (payCount - other.getPayCount());
        }
        return 0;
    }

    public CategorySortKey(long clickCount, long orderCount, long payCount) {
        this.clickCount = clickCount;
        this.orderCount = orderCount;
        this.payCount = payCount;
    }

    public CategorySortKey() {
    }

    public long getClickCount() {
        return clickCount;
    }

    public void setClickCount(long clickCount) {
        this.clickCount = clickCount;
    }

    public long getOrderCount() {
        return orderCount;
    }

    public void setOrderCount(long orderCount) {
        this.orderCount = orderCount;
    }

    public long getPayCount() {
        return payCount;
    }

    public void setPayCount(long payCount) {
        this.payCount = payCount;
    }
}
