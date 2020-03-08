package com.imooc.bigdata.hadoop.mr.project;

public class IPInfo {
    private String ip;
    private String country;
    private String province;
    private String city;

    public String getIp() {
        return ip;
    }

    public void setIp(String ip) {
        this.ip = ip;
    }

    public String getCountry() {
        return country == null ? "-" : country;
    }

    public void setCountry(String country) {
        this.country = country;
    }

    public String getProvince() {
        return province == null ? "-" : province;
    }

    public void setProvince(String province) {
        this.province = province;
    }

    public String getCity() {
        return city == null ? "-" : city;
    }

    public void setCity(String city) {
        this.city = city;
    }

    @Override
    public String toString() {
        return "IPInfo{" +
                "ip='" + ip + '\'' +
                ", country='" + country + '\'' +
                ", province='" + province + '\'' +
                ", city='" + city + '\'' +
                '}';
    }
}
