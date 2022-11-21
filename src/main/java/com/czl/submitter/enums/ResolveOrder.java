package com.czl.submitter.enums;

/**
 * Author: CHEN ZHI LING
 * Date: 2022/6/1
 * Description:
 */
public enum ResolveOrder {
    /**
     * parent-first
     */
    PARENT_FIRST("parent-first", 0),
    /**
     * child-first
     */
    CHILD_FIRST("child-first", 1);

    private final String name;

    private final Integer value;

    ResolveOrder(String name, Integer value) {
        this.name = name;
        this.value = value;
    }

    public static ResolveOrder of(Integer value) {
        for (ResolveOrder order : values()) {
            if (order.value.equals(value)) {
                return order;
            }
        }
        return null;
    }

    public String getName() {
        return name;
    }

    public Integer getValue() {
        return value;
    }
}
