package com.imooc.bigdata.hadoop.mr.project.utils;

import org.junit.Test;

public class IPParserTest {
    @Test
    public void testIP() {
        final IPParser.RegionInfo regionInfo = IPParser.getInstance().analyseIp("123.116.60.97");
        System.out.println(regionInfo);
    }

    @Test
    public void local(){
        final IPParser.RegionInfo regionInfo = IPParser.getInstance().analyseIp("171.44.101.160");
        System.out.println(regionInfo);
    }
}
