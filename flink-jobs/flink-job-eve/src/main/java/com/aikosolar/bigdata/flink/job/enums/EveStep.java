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

        @Override
        public EveStep next(String eqpType) {
            if ("DF".equalsIgnoreCase(eqpType)) {
                return next();
            }
            return next();
        }
    },
    CONDITION {
        @Override
        public EveStep next() {
            return PROCESS;
        }

        @Override
        public EveStep next(String eqpType) {
            if ("DF".equalsIgnoreCase(eqpType)) {
                return next();
            }
            return next();
        }
    },
    PROCESS {
        @Override
        public EveStep next() {
            return CLEAN;
        }

        @Override
        public EveStep next(String eqpType) {
            if ("DF".equalsIgnoreCase(eqpType)) {
                return next();
            }
            return next();
        }
    },
    CLEAN {
        @Override
        public EveStep next() {
            return UNLOAD;
        }

        @Override
        public EveStep next(String eqpType) {
            if ("DF".equalsIgnoreCase(eqpType)) {
                return next();
            }
            return next();
        }
    },
    UNLOAD {
        @Override
        public EveStep next() {
            return CHANGE;
        }

        @Override
        public EveStep next(String eqpType) {
            if ("DF".equalsIgnoreCase(eqpType)) {
                return next();
            }
            return next();
        }
    },
    CHANGE {
        @Override
        public EveStep next() {
            return LOAD;
        }

        @Override
        public EveStep next(String eqpType) {
            if ("DF".equalsIgnoreCase(eqpType)) {
                return next();
            }
            return next();
        }
    },
    ;

    /**
     * 下一个step(循环状态机)
     */
    protected abstract EveStep next();

    /**
     * 根据设备类型获取下一个step
     * 根据具体情况调整
     */
    public abstract EveStep next(String eqpType);
}
