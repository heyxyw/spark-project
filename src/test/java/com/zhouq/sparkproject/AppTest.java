package com.zhouq.sparkproject;

import com.zhouq.sparkproject.conf.ConfigurationManager;
import org.junit.Test;

/**
 * //todo
 *
 * @Author: zhouq
 * @Date: 2019-06-16
 * @Version: 1.0.0
 * @Since: JDK 1.8
 */
public class AppTest {

    @Test
    public void ConfigurationManagerTest(){
        String test1 = ConfigurationManager.getProperty("test1");
        String test2 = ConfigurationManager.getProperty("test2");
        System.out.println(test1);
        System.out.println(test2);
    }



}
