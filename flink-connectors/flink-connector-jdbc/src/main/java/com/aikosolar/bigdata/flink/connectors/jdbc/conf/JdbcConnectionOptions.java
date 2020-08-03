package com.aikosolar.bigdata.flink.connectors.jdbc.conf;

import java.io.Serializable;

/**
 * @author carlc
 */
public class JdbcConnectionOptions implements Serializable {
    private String driverName;
    private String url;
    private String username;
    private String password;
    private int maxRetries;
    private int checkTimeoutSeconds;

    private JdbcConnectionOptions(String driverName, String url, String username, String password,
                                  int maxRetries, int checkTimeoutSeconds) {
        this.driverName = driverName;
        this.url = url;
        this.username = username;
        this.password = password;
        this.maxRetries = maxRetries;
        this.checkTimeoutSeconds = checkTimeoutSeconds;
    }


    public String getDriverName() {
        return driverName;
    }

    public String getUrl() {
        return url;
    }

    public String getUsername() {
        return username;
    }

    public String getPassword() {
        return password;
    }

    public int getMaxRetries() {
        return maxRetries;
    }

    public int getCheckTimeoutSeconds() {
        return checkTimeoutSeconds;
    }


    /**
     * Builder for {@link JdbcConnectionOptions}.
     */
    public static class Builder {
        private static final int DEFAULT_MAX_RETRY_TIMES = 3;
        private static final int DEFAULT_CHECK_TIMEOUT_SECONDS = 60;

        private String url;
        private String driverName;
        private String username;
        private String password;

        private int maxRetries = DEFAULT_MAX_RETRY_TIMES;
        private int checkTimeoutSeconds = DEFAULT_CHECK_TIMEOUT_SECONDS;

        public Builder withUrl(String url) {
            this.url = url;
            return this;
        }

        public Builder withDriverName(String driverName) {
            this.driverName = driverName;
            return this;
        }

        public Builder withUsername(String username) {
            this.username = username;
            return this;
        }

        public Builder withPassword(String password) {
            this.password = password;
            return this;
        }

        public JdbcConnectionOptions build() {
            return new JdbcConnectionOptions(driverName, url, username, password, maxRetries, checkTimeoutSeconds);
        }
    }
}



