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
public class ServicesStack implements Serializable {

    @Column(name = "id")
    private String id;

    @Column(name = "region")
    private String region;

    @Column(name = "loglevel")
    private String loglevel;

    @Column(name = "service")
    private String service;

    @Column(name = "ts")
    private String ts;

    public ServicesStack(String id, String region, String loglevel, String service, String ts) {
        this.id = id;
        this.region = region;
        this.loglevel = loglevel;
        this.service = service;
        this.ts = ts;
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getRegion() {
        return region;
    }

    public void setRegion(String region) {
        this.region = region;
    }

    public String getLoglevel() {
        return loglevel;
    }

    public void setLoglevel(String loglevel) {
        this.loglevel = loglevel;
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
