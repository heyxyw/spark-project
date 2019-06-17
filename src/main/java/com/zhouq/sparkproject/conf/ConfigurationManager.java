package com.zhouq.sparkproject.conf;

import org.apache.spark.sql.columnar.INT;

import java.io.InputStream;
import java.util.Properties;

/**
 * 配置管理组件
 * 1.配置管理组件，读取配置文件，统一提供对外界获取某个配置key 对应的value 方法
 * 2.如果对于一些特别复杂的配置管理组件，那么可能需要使用一些软件设计模式。比如单利、解释器可能需要管理多个不同的properties，甚至是xml 类型的配置文件。
 *
 *
 * @Author: zhouq
 * @Date: 2019-06-16
 * @Version: 1.0.0
 * @Since: JDK 1.8
 */
public class ConfigurationManager {

    private static Properties prop = new Properties();


    /**
     * 静态代码块
     * Java 中，每一个类第一次加载的时候，就会被Java 的虚拟机（JVM）中的类加载起，从磁盘上的.class 文件中加载出来。
     * 然后为每个类都会构建一个Class 对象，就代表类这个类。
     *
     * 每个类在第一次加载的时候，都会进行自身的初始化，那么类初始化的时候，就会执行哪些操作呢？
     * 类第一次使用的时候，就会加载，加载的时候就会初始化，初始化类的时候就会执行类的静态代码块。
     *
     * 所以我们这里就可以在静态代码块中写我们读取配置文件的代码。
     *
     * 这样在第一次外界调用 ConfigurationManager 类的静态方法的时候，就会加载配置文件中的数据了。
     *
     * 而且放在静态代码块中又一个好处就是类的初始化是在JVM 生命周期内，有且仅有一次，也就是配置文件只会加载一次，然后以后就是重复使用，效率高。不用加载多次。
     *
     *
     *
     */
    static {
        try {

            //通过一个"类名.class" 的港式，就可以获取到这个类在JVM 中的class对象
            // 然后再通过这个 Class 对象的getClassLoader() 方法，就可以获取到加载这个类的JVM 中的类加载器（ClassLoader）
            // 然后调用类加载器的 getResourceAsStream() 的这个方法。就可以加载资源露肩中指定的文件了。
            //最终可以获取到一个针对指定文件的输入流InputStream。
            InputStream in = ConfigurationManager.class.getClassLoader().getResourceAsStream("my.properties");

            // 调用 Properties的 load() 方法。给他传入一个 InputStream 输入流
            // 即可将文件中的符合"key=value" 格式的配置文件项，都加载到Properties 对象中
            // 加载过后，此时 Properties 对象中就有了配置文件中所有的 key-value 了。
            // 然后外界就可以通过 Properties 对象获取指定 key 对应的value 了。
            prop.load(in);
        }catch (Exception e){
            e.printStackTrace();
        }
    }

    /**
     * 获取制定key 对应的value
     *
     * 第一次外界调用的时候，调用 ConfigurationManager 类的 getProperties 静态方法时，JVM 内部会发现 ConfigurationManger 类还不在 JVM 中。
     *
     * 此时 JVM 就会使用自己的 ClassLoader(类加载器)，对应的类所在的磁盘文件（.class 文件） 中去加载 ConfigurationManger 类，到JVM 内存中来。
     *
     * 并根据类内部的一些信息，去创建一个 Class 对象，Class 中包含了类的元信息，包含类类有哪些 field(Properties prop) ;有哪些方法 （getProperty）
     *
     * 加载 ConfigurationManger 类的时候，还会初始化这个类，那么此时就会执行 类的static （静态代码块）中的内容，也就是我们写在静态代码块中的内容。
     *
     * 就会把 my.properties 文件的类容，加载到 Properties 对象中。
     *
     * 下一次外界代码再调用 ConfigurationManger 类的  getProperty() 方法的时候，就不会再次加载类和初始化类了。
     *
     * @param key
     * @return
     */
    public static String getProperty(String key){
        return prop.getProperty(key);
    }

    /**
     * 获取Integer 类型的value值
     * @param key
     * @return
     */
    public static Integer getInteger(String key){
        return Integer.valueOf(getProperty(key));
    }


}
