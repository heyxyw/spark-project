package com.zhouq.sparkproject

/**
 * Scala 版本的二次排序
 *
 * @Author: zhouq
 * @Date: 2019/10/27
 */
class SortKey(val clickCount: Int,
              val orderCount: Int,
              val payCount: Int)
  extends Ordered[SortKey] with Serializable {

  override def compare(that: SortKey): Int = {
    if (clickCount - that.clickCount != 0) {
      clickCount - that.clickCount
    } else if (orderCount - that.orderCount != 0) {
      orderCount - that.orderCount
    } else if (payCount - that.payCount != 0) {
      payCount - that.payCount
    } else {
      0
    }
  }

}
