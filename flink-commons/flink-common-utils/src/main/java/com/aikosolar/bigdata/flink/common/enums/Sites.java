package com.aikosolar.bigdata.flink.common.enums;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Arrays;
import java.util.Locale;

/**
 * 基地枚举
 *
 * @author carlc
 */
public enum Sites {

    /**
     * 浙江
     */
    ZJ {
        @Override
        public boolean accept(String site) {
            return site.toUpperCase().trim().startsWith("Z");
        }

        @Override
        public String toFactoryId0(String site) {
            if ("Z2".equals(site) || "Z3".equals(site)) {
                return "4";
            }
            return null;
        }
    },

    /**
     * 天津
     */
    TJ {
        @Override
        public boolean accept(String site) {
            return site.toUpperCase().trim().startsWith("T");
        }

        @Override
        public String toFactoryId0(String site) {
            if ("T1".equals(site)) {
                return "3";
            }
            return null;

        }
    },

    /**
     * 广东
     */
    GD {
        @Override
        public boolean accept(String site) {
            return site.toUpperCase().trim().startsWith("G");
        }

        @Override
        public String toFactoryId0(String site) {
            if ("G1".equals(site)) {
                return "1";
            }
            return null;
        }

        @Override
        public String toShift(LocalDateTime l) {
            int hour = l.getHour();
            int minute = l.getMinute();

            // 7点边界条件
            if (hour == 7) {
                if (minute >= 30) {
                    return l.toLocalDate().format(fmt) + "-D";
                } else {
                    return l.toLocalDate().minusDays(1).format(fmt) + "-N";
                }
            }
            // 19点边界条件
            if (hour == 19) {
                return l.toLocalDate().format(fmt) + (minute >= 30 ? "-N" : "-D");
            }
            // 其他
            if (hour > 7 && hour < 19) {
                return l.toLocalDate().format(fmt) + "-D";
            }
            return l.toLocalDate().minusDays(hour < 7 ? 1 : 0).format(fmt) + "-N";
        }

    },
    ;

    private static final DateTimeFormatter fmt = DateTimeFormatter.ofPattern("yyyy-MM-dd", Locale.CHINA);
    private static final String UNKNOWN_FACTORY_ID = "Other";

    /**
     * 时间转换为班次(默认实现:08:00-20:00 => D)
     * 浙江基地:8:00-20:00
     * 天津基地:8:00-20:00
     * 广东基地:7:30-19:30
     */
    public String toShift(LocalDateTime l) {
        int h = l.getHour();
        if (h >= 8 && (h < 20)) {
            return l.toLocalDate().format(fmt) + "-D";
        } else if (h < 8) {
            return l.toLocalDate().minusDays(1).format(fmt) + "-N";
        } else {
            return l.toLocalDate().format(fmt) + "-N";
        }
    }

    public abstract boolean accept(String site);

    public abstract String toFactoryId0(String site);

    public static String toFactoryId(String site) {
        Sites s = toSite(site);
        if (s == null) {
            return UNKNOWN_FACTORY_ID;
        }
        String id = s.toFactoryId0(site.toUpperCase().trim());
        if (id == null) {
            id = UNKNOWN_FACTORY_ID;
        }
        return id;
    }

    public static Sites toSite(String site) {
        if (site == null || "".equals(site.trim())) {
            return null;
        }
        return Arrays.stream(Sites.values()).filter(s -> s.accept(site)).findFirst().orElse(null);
    }
}
