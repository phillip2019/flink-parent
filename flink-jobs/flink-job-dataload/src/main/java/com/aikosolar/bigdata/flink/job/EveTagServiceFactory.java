package com.aikosolar.bigdata.flink.job;

import org.apache.commons.lang3.StringUtils;

/**
 * @author carlc
 */
public class EveTagServiceFactory {
    public static EveTagService getEveTagService(String type) {
        if (StringUtils.isBlank(type)) {
            return null;
        }

        switch (type) {
            case "DF":
                return DFEveTagService.me();
            case "PE":
                return PEEveTagService.me();
            case "PR":
                return PREveTagService.me();
            case "TA":
                return TAEveTagService.me();
            default: {
                try {
                    Class<?> clazz = Class.forName(type);
                    // 必须要有我们约定的静态方法 me()
                    return (EveTagService) clazz.getMethod("me").invoke(null);
                } catch (Exception e) {
                    return null;
                }
            }
        }
    }
}
