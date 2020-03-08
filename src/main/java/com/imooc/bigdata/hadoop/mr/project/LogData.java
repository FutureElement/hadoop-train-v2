package com.imooc.bigdata.hadoop.mr.project;

public class LogData {
    private IPInfo ipInfo;
    private String url;
    private String pageId;
    private String time;
    private String sessionId;

    public IPInfo getIpInfo() {
        return ipInfo;
    }

    public void setIpInfo(IPInfo ipInfo) {
        this.ipInfo = ipInfo;
    }

    public String getUrl() {
        return url;
    }

    public void setUrl(String url) {
        this.url = url;
    }

    public String getPageId() {
        return pageId == null ? "-" : pageId;
    }

    public void setPageId(String pageId) {
        this.pageId = pageId;
    }

    public String getTime() {
        return time;
    }

    public void setTime(String time) {
        this.time = time;
    }

    public String getSessionId() {
        return sessionId;
    }

    public void setSessionId(String sessionId) {
        this.sessionId = sessionId;
    }

    @Override
    public String toString() {
        return "LogData{" +
                "ipInfo=" + ipInfo +
                ", url='" + url + '\'' +
                ", pageId='" + pageId + '\'' +
                ", time='" + time + '\'' +
                ", sessionId='" + sessionId + '\'' +
                '}';
    }
}
