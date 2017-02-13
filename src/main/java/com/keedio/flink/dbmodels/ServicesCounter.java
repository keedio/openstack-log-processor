package com.keedio.flink.dbmodels;

import com.datastax.driver.mapping.annotations.Column;
import com.datastax.driver.mapping.annotations.Table;

import java.io.Serializable;

/**
 * Created by luislazaro on 13/2/17.
 * lalazaro@keedio.com
 * Keedio
 */

@Table(keyspace = "redhatpoc", name = "cassandraconnectorexample")
public class ServicesCounter implements Serializable {

    @Column(name = "id")
    private String id;

    @Column(name = "loglevel")
    private String loglevel;

    @Column(name = "az")
    private String az;

    @Column(name = "region")
    private String region;

    @Column(name = "service")
    private String service;

    @Column(name = "ts")
    private String ts;

    public ServicesCounter(String id, String loglevel, String az, String region, String service, String ts) {
        this.id = id;
        this.loglevel = loglevel;
        this.az = az;
        this.region = region;
        this.service = service;
        this.ts = ts;
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getLoglevel() {
        return loglevel;
    }

    public void setLoglevel(String loglevel) {
        this.loglevel = loglevel;
    }

    public String getAz() {
        return az;
    }

    public void setAz(String az) {
        this.az = az;
    }

    public String getRegion() {
        return region;
    }

    public void setRegion(String region) {
        this.region = region;
    }

    public String getService() {
        return service;
    }

    public void setService(String service) {
        this.service = service;
    }

    public String getTs() {
        return ts;
    }

    public void setTs(String ts) {
        this.ts = ts;
    }
}
