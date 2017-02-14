package com.keedio.flink.dbmodels;

import com.datastax.driver.mapping.annotations.Column;
import com.datastax.driver.mapping.annotations.QueryParameters;
import com.datastax.driver.mapping.annotations.Table;


import java.io.Serializable;

/**
 * Created by luislazaro on 13/2/17.
 * lalazaro@keedio.com
 * Keedio
 */

@Table(keyspace = "redhatpoc", name = "counters_nodes_test")
public class NodesCounter implements Serializable {

    @Column(name = "id")
    private String id;

    @Column(name = "loglevel")
    private String loglevel;

    @Column(name = "az")
    private String az;

    @Column(name = "region")
    private String region;

    @Column(name = "node_type")
    private String node_type;

    @Column(name = "ts")
    private String ts;

    public NodesCounter(String id, String loglevel, String az, String region, String node_type, String ts) {
        this.id = id;
        this.loglevel = loglevel;
        this.az = az;
        this.region = region;
        this.node_type = node_type;
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

    public String getNode_type() {
        return node_type;
    }

    public void setNode_type(String node_type) {
        this.node_type = node_type;
    }

    public String getTs() {
        return ts;
    }

    public void setTs(String ts) {
        this.ts = ts;
    }

    @Override
    public String toString() {
        return "NodesCounter{" +
                "id='" + id + '\'' +
                ", loglevel='" + loglevel + '\'' +
                ", az='" + az + '\'' +
                ", region='" + region + '\'' +
                ", node_type='" + node_type + '\'' +
                ", ts='" + ts + '\'' +
                '}';
    }
}
