package com.aikosolar.bigdata.flink.job;

/**
 * @author carlc
 */
public class EveTagServiceFactory {
    public static EveTagService getEveTagService(String type) {
        switch (type) {
            case "DF":
                return DFEveTagService.me();
            case "PE":
                return PEEveTagService.me();
            case "PR":
                return PREveTagService.me();
            case "TA":
                return TAEveTagService.me();
            default:
                throw new IllegalArgumentException("不支持的类型：" + type);
        }
    }
}
