package com.keedio.flink.dbmodels;

import com.datastax.driver.mapping.annotations.Column;
import com.datastax.driver.mapping.annotations.Table;

import java.io.Serializable;

/**
 * Created by luislazaro on 12/2/17.
 * lalazaro@keedio.com
 * Keedio
 */

@Table(keyspace = "redhatpoc", name = "cassandraconnectorexample")
public class PojoSampleJava implements Serializable{
    private static final long serialVersionUID = 1038054554690916991L;

    @Column(name = "id")
    private String id;
    @Column(name = "text")
    private String text;

    public PojoSampleJava(String id, String text){
        this.id = id;
        this.text = text;
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getText() {
        return text;
    }

    public void setText(String text) {
        this.text = text;
    }
}
