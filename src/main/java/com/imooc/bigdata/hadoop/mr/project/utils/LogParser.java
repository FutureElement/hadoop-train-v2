package com.imooc.bigdata.hadoop.mr.project.utils;


import com.imooc.bigdata.hadoop.mr.project.IPInfo;
import com.imooc.bigdata.hadoop.mr.project.LogData;
import org.apache.commons.lang.StringUtils;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class LogParser {
    private static LogParser instance = new LogParser();

    private LogParser() {
    }

    public static LogParser getInstance() {
        return instance;
    }

    public IPInfo parseIPV2(String log) {
        final IPInfo ipInfo = new IPInfo();
        if (StringUtils.isNotBlank(log)) {
            final String[] splits = log.split("\t");
            ipInfo.setIp(splits[0]);
            ipInfo.setCountry(splits[1]);
            ipInfo.setProvince(splits[2]);
            ipInfo.setCity(splits[3]);
        }
        return ipInfo;
    }

    public LogData parseAllV2(String log) {
        final LogData logData = new LogData();
        if (StringUtils.isNotBlank(log)) {
            final String[] splits = log.split("\t");
            final IPInfo ipInfo = new IPInfo();
            ipInfo.setIp(splits[0]);
            ipInfo.setCountry(splits[1]);
            ipInfo.setProvince(splits[2]);
            ipInfo.setCity(splits[3]);
            logData.setIpInfo(ipInfo);

            logData.setUrl(splits[4]);
            logData.setTime(splits[5]);
            logData.setSessionId(splits[6]);
            logData.setPageId(splits[7]);
        }
        return logData;
    }

    public IPInfo parseIP(String log) {
        final IPInfo ipInfo = new IPInfo();
        if (StringUtils.isNotBlank(log)) {
            final IPParser ipParser = IPParser.getInstance();
            final String[] splits = log.split("\001");
            final String ip = splits[13];
            final IPParser.RegionInfo regionInfo = ipParser.analyseIp(ip);
            if (regionInfo != null) {
                ipInfo.setIp(ip);
                ipInfo.setCountry(regionInfo.getCountry());
                ipInfo.setProvince(regionInfo.getProvince());
                ipInfo.setCity(regionInfo.getCity());
            }
        }
        return ipInfo;
    }

    public String getPageId(String log) {
        String url = null;
        if (StringUtils.isNotBlank(log)) {
            final String[] splits = log.split("\001");
            url = splits[1];
        }
        return getPageIdByUrl(url);
    }

    public LogData parseAll(String log) {
        final LogData logData = new LogData();
        if (StringUtils.isNotBlank(log)) {
            final IPParser ipParser = IPParser.getInstance();
            final String[] splits = log.split("\001");
            final String ip = splits[13];
            final IPParser.RegionInfo regionInfo = ipParser.analyseIp(ip);
            if (regionInfo != null) {
                final IPInfo ipInfo = new IPInfo();
                ipInfo.setIp(ip);
                ipInfo.setCountry(regionInfo.getCountry());
                ipInfo.setProvince(regionInfo.getProvince());
                ipInfo.setCity(regionInfo.getCity());
                logData.setIpInfo(ipInfo);
            }
            String url = splits[1];
            logData.setUrl(url);
            logData.setPageId(getPageIdByUrl(url));
            logData.setSessionId(splits[10]);
            logData.setTime(splits[17]);
        }
        return logData;
    }

    private String getPageIdByUrl(String url) {
        if (StringUtils.isBlank(url)) {
            return null;
        }
        Pattern pat = Pattern.compile("topicId=[0-9]+");
        Matcher matcher = pat.matcher(url);

        String pageId = null;
        if (matcher.find()) {
            pageId = matcher.group().split("topicId=")[1];
        }
        return pageId;
    }
}
