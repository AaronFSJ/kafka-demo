package com.marty.entity;

import java.io.Serializable;
import java.math.BigDecimal;

/**
 * @Author: Aaron
 * @Descprition:
 * @Date: Create in 2017/12/20 15:30
 * @Modyfied By:
 */

public class User implements Serializable {
    private String id;
    private String name;
    private int sex;
    private BigDecimal solor;

    public User(){}

    public User(String id,String name,int sex,BigDecimal solor){
        this.id = id;
        this.name = name;
        this.sex = sex;
        this.solor = solor;
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public int getSex() {
        return sex;
    }

    public void setSex(int sex) {
        this.sex = sex;
    }

    public BigDecimal getSolor() {
        return solor;
    }

    public void setSolor(BigDecimal solor) {
        this.solor = solor;
    }
}
