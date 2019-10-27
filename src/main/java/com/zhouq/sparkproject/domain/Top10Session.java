package com.zhouq.sparkproject.domain;

/**
 * top10 活跃session
 *
 * @Author: zhouq
 * @Date: 2019/10/27
 */
public class Top10Session {
    private long taskid;
    private long categoryid;
    private String sessionid;
    private long clickCount;

    public Top10Session(long taskid) {
        this.taskid = taskid;
    }

    public Top10Session(long taskid, long categoryid, String sessionid, long clickCount) {
        this.taskid = taskid;
        this.categoryid = categoryid;
        this.sessionid = sessionid;
        this.clickCount = clickCount;
    }

    public long getTaskid() {
        return taskid;
    }

    public void setTaskid(long taskid) {
        this.taskid = taskid;
    }

    public long getCategoryid() {
        return categoryid;
    }

    public void setCategoryid(long categoryid) {
        this.categoryid = categoryid;
    }

    public String getSessionid() {
        return sessionid;
    }

    public void setSessionid(String sessionid) {
        this.sessionid = sessionid;
    }

    public long getClickCount() {
        return clickCount;
    }

    public void setClickCount(long clickCount) {
        this.clickCount = clickCount;
    }
}
