package com.czl.submitter.enums;

import java.io.Serializable;

/**
 * Author: CHEN ZHI LING
 * Date: 2022/5/26
 */
public enum ExecutionMode implements Serializable {


    LOCAL(0, "local"),

    REMOTE(1, "remote"),

    FLINK_YARN_SESSION(2, "yarn-session"),

    FLINK_YARN_PER_JOB(3,"yarn-per-job");

    private final Integer mode;

    private final String name;

    ExecutionMode(Integer mode, String name) {
        this.mode = mode;
        this.name = name;
    }


    public int getMode() {
        return mode;
    }

    public String getName() {
        return name;
    }

    public static ExecutionMode of(Integer value) {
        for (ExecutionMode executionMode : values()) {
            if (executionMode.mode.equals(value)) {
                return executionMode;
            }
        }
        return null;
    }
}
