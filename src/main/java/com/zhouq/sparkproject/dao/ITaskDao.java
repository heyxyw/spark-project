package com.zhouq.sparkproject.dao;

import com.zhouq.sparkproject.domain.Task;

/**
 * 任务管理 Dao 接口
 *
 * @Author: zhouq
 * @Date: 2019-06-16
 */
public interface ITaskDao {
    /**
     * 根据主键查询任务
     * @param id  主键
     * @return 任务
     */
    Task findById(long id);
}
