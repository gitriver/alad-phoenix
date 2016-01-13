package me.alad.phoenix.datadeal;

public class DayHourStat {
    private int day;

    private int hour;

    private int pv;

    private int au;

    private int ru;


    public int getDay() {
        return day;
    }


    public void setDay(int day) {
        this.day = day;
    }


    public int getHour() {
        return hour;
    }


    public void setHour(int hour) {
        this.hour = hour;
    }


    public int getPv() {
        return pv;
    }


    public void setPv(int pv) {
        this.pv = pv;
    }


    public int getAu() {
        return au;
    }


    public void setAu(int au) {
        this.au = au;
    }


    public int getRu() {
        return ru;
    }


    public void setRu(int ru) {
        this.ru = ru;
    }


    @Override
    public String toString() {
        return day + "," + hour + "," + pv + "," + au + "," + ru;
    }

}
