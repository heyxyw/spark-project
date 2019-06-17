package com.zhouq.sparkproject.dao.impl;

import com.zhouq.sparkproject.dao.ITaskDao;
import com.zhouq.sparkproject.domain.Task;
import com.zhouq.sparkproject.jdbc.JDBCHelper;

import java.sql.ResultSet;

/**
 * 任务管理 Dao 实现类
 *
 * @Author: zhouq
 * @Date: 2019-06-16
 */
public class TaskDaoImpl implements ITaskDao {
    /**
     * 根据主键查询任务
     * @param id  主键
     * @return 任务
     */
    @Override
    public Task findById(long id) {
        final Task task = new Task();

        String sql = "select * from task where task_id = ?";

        JDBCHelper jdbcHelper = JDBCHelper.getInstance();

        jdbcHelper.executeQuery(sql, new Object[]{id}, new JDBCHelper.QueryCallback() {
            @Override
            public void process(ResultSet rs) throws Exception {
                if (rs.next()){
                    long taskId = rs.getLong(1);
                    String taskName = rs.getString(2);
                    String createTime = rs.getString(3);
                    String startTime = rs.getString(4);
                    String finishTime = rs.getString(5);
                    String taskType = rs.getString(6);
                    String taskStatus = rs.getString(7);
                    String taskParam = rs.getString(8);

                    task.setTaskId(taskId);
                    task.setTaskName(taskName);
                    task.setCreateTime(createTime);
                    task.setStartTime(startTime);
                    task.setFinishTime(finishTime);
                    task.setTaskType(taskType);
                    task.setTaskStatus(taskStatus);
                    task.setTaskParam(taskParam);
                }
            }
        });

        return task;
    }
}
