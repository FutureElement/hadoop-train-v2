package com.imooc.bigdata.hadoop.mr.project.utils;


import com.imooc.bigdata.hadoop.mr.project.IPInfo;
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
