package org.example.demo01.beans;

import java.io.Serializable;

public class Order implements Serializable {
    int amount;
    String id;

    public Order(String valueOf, int i) {
        this.id = valueOf;
        this.amount = i;
    }

    public int getAmount() {
        return amount;
    }

    public void setAmount(int amount) {
        this.amount = amount;
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }
}
