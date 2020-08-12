package com.aikosolar.bigdata.flink.job.enums;

/**
 * @author carlc
 */
public enum EveStep {

    LOAD {
        @Override
        public EveStep next() {
            return CONDITION;
        }
    },
    CONDITION {
        @Override
        public EveStep next() {
            return PROCESS;
        }
    },
    PROCESS {
        @Override
        public EveStep next() {
            return CLEAN;
        }
    },
    CLEAN {
        @Override
        public EveStep next() {
            return UNLOAD;
        }
    },
    UNLOAD {
        @Override
        public EveStep next() {
            return CHANGE;
        }
    },
    CHANGE {
        @Override
        public EveStep next() {
            return LOAD;
        }
    },
    ;

    /**
     * 下一个step(循环状态机)
     */
    public abstract EveStep next();
}
