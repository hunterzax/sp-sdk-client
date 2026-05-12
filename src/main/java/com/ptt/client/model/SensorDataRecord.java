package com.ptt.client.model;

import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * POJO representing a sensor data record from Kafka.
 */
public class SensorDataRecord {
    private String tagname;

    @JsonProperty("cur_value")
    private double curValue;
    private String time;
    private int gw;

    @JsonProperty("avg_hour_current")
    private Double avgHourCurrent;
    @JsonProperty("avg_hour_previous")
    private Double avgHourPrevious;
    @JsonProperty("avg_day_current")
    private Double avgDayCurrent;
    @JsonProperty("avg_day_previous")
    private Double avgDayPrevious;
    @JsonProperty("flag_fresh")
    private String flagFresh;
    @JsonProperty("hillow_state")
    private String hillowState;

    public SensorDataRecord() {
    }

    public SensorDataRecord(String tagname, double curValue, String time, int gw) {
        this.tagname = tagname;
        this.curValue = curValue;
        this.time = time;
        this.gw = gw;
    }

    public String getTagname() {
        return tagname;
    }

    public void setTagname(String tagname) {
        this.tagname = tagname;
    }

    public double getCurValue() {
        return curValue;
    }

    public void setCurValue(double curValue) {
        this.curValue = curValue;
    }

    public String getTime() {
        return time;
    }

    public void setTime(String time) {
        this.time = time;
    }

    public int getGw() {
        return gw;
    }

    public void setGw(int gw) {
        this.gw = gw;
    }

    public Double getAvgHourCurrent() {
        return avgHourCurrent;
    }

    public void setAvgHourCurrent(Double avgHourCurrent) {
        this.avgHourCurrent = avgHourCurrent;
    }

    public Double getAvgHourPrevious() {
        return avgHourPrevious;
    }

    public void setAvgHourPrevious(Double avgHourPrevious) {
        this.avgHourPrevious = avgHourPrevious;
    }

    public Double getAvgDayCurrent() {
        return avgDayCurrent;
    }

    public void setAvgDayCurrent(Double avgDayCurrent) {
        this.avgDayCurrent = avgDayCurrent;
    }

    public Double getAvgDayPrevious() {
        return avgDayPrevious;
    }

    public void setAvgDayPrevious(Double avgDayPrevious) {
        this.avgDayPrevious = avgDayPrevious;
    }

    public String getFlagFresh() {
        return flagFresh;
    }

    public void setFlagFresh(String flagFresh) {
        this.flagFresh = flagFresh;
    }

    public String getHillowState() {
        return hillowState;
    }

    public void setHillowState(String hillowState) {
        this.hillowState = hillowState;
    }

    @Override
    public String toString() {
        return "SensorDataRecord{" +
                "tagname='" + tagname + '\'' +
                ", curValue=" + curValue +
                ", time='" + time + '\'' +
                ", gw=" + gw +
                ", flagFresh='" + flagFresh + '\'' +
                ", hillowState='" + hillowState + '\'' +
                '}';
    }
}
