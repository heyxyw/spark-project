package com.zhouq.sparkproject;

import com.zhouq.sparkproject.dao.ITaskDao;
import com.zhouq.sparkproject.dao.impl.DAOFactory;
import com.zhouq.sparkproject.domain.Task;
import org.junit.Test;

/**
 * Create by zhouq on 2019/6/17
 */
public class TaskDaoTest {

    @Test
    public void findById(){
        ITaskDao taskDao = DAOFactory.getTaskDao();

        Task task = taskDao.findById(1L);

        System.out.println(task);

    }
}
