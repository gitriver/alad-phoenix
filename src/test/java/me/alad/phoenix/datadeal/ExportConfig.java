package me.alad.phoenix.datadeal;

public class ExportConfig {
    private String sql;
    private String path;
    private String fieldDelimiter;
    private boolean isAppend;

    
    public boolean isAppend() {
        return isAppend;
    }


    public void setAppend(boolean isAppend) {
        this.isAppend = isAppend;
    }


    public String getSql() {
        return sql;
    }


    public void setSql(String sql) {
        this.sql = sql;
    }


    public String getPath() {
        return path;
    }


    public void setPath(String path) {
        this.path = path;
    }


    public String getFieldDelimiter() {
        return fieldDelimiter;
    }


    public void setFieldDelimiter(String fieldDelimiter) {
        this.fieldDelimiter = fieldDelimiter;
    }

}
