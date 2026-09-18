package com.alsoapp.rhapsody.rdk.filter.redis;

import com.orchestral.rhapsody.module.Configuration;

/**
 * Redis 连接/执行配置。由过滤器在 doConfigure() 中从 IDE 属性解析而来。
 * 同时服务 cluster（经 Predixy 代理）与 sentinel 两种模式。
 */
public class RedisConfig {

    /** 连接模式 */
    public enum Mode { CLUSTER, SENTINEL }

    private final Mode mode;

    // ---- cluster / 通用 ----
    private String host = "10.254.147.60";   // 默认 Predixy 代理入口
    private int port = 7617;
    private String password = "cdr@lhim.org"; // 默认 Predixy 前端密码
    private int timeout = 2000;

    // ---- sentinel ----
    private String masterName;
    private String sentinelHosts;             // "host1,host2"（端口见 sentinelPort）
    private String sentinelPassword;         // 哨兵鉴权（可选）
    private int sentinelPort = 36379;         // 哨兵端口（独立配置项）

    // ---- 命令模板（公共）----
    private String command = "SET";
    private String keyTemplate;
    private String valueTemplate;
    private String hashField;
    private String channel;
    private String resultProperty = "RedisResult";
    private OnError onError = OnError.ERROR_QUEUE;

    // ---- Redis 库编号（公共）----
    private int dbIndex = 0;   // 0-15, 默认 db0

    // ---- TTL / ACL 用户名（公共）----
    private int ttl = 0;                     // 过期秒数, 0=不过期(静态回退值)
    private String ttlTemplate = "0";       // 过期秒数模板, 可含 ${prop}/$prop, 按每条消息解析
    private boolean usernameEnabled = false;
    private String username;                 // ACL 用户名, 未启用/未填则为 null

    public enum OnError {
        ERROR_QUEUE,   // 抛 FilterProcessingException -> 进错误队列
        IGNORE,        // 仅记录日志，继续
        EXCEPTION      // 抛 MessageException
    }

    private RedisConfig(Mode mode) {
        this.mode = mode;
    }

    /** 从过滤器配置解析（cluster 版） */
    public static RedisConfig fromClusterConfig(Configuration config) {
        RedisConfig c = new RedisConfig(Mode.CLUSTER);
        c.host = str(config, "HOST", c.host);
        c.port = intv(config, "PORT", c.port);
        c.password = str(config, "PASSWORD", c.password);
        c.timeout = intv(config, "TIMEOUT", c.timeout);
        fillCommon(c, config);
        return c;
    }

    /** 从过滤器配置解析（sentinel 版） */
    public static RedisConfig fromSentinelConfig(Configuration config) {
        RedisConfig c = new RedisConfig(Mode.SENTINEL);
        c.masterName = str(config, "MASTER_NAME", null);
        c.sentinelHosts = str(config, "SENTINEL_HOSTS", null);
        c.sentinelPassword = str(config, "SENTINEL_PASSWORD", null);
        c.sentinelPort = intv(config, "PORT", c.sentinelPort);
        // SENTINEL 模式下 JedisSentinelPool 直接吃 SENTINEL_HOSTS（host:port 列表）与 MASTER_NAME，
        // 由哨兵发现 master 真实地址后直连。host/port 默认值(10.254.147.60:7617)对 sentinel 模式无意义，保留不影响。
        c.timeout = intv(config, "TIMEOUT", c.timeout);
        fillCommon(c, config);
        return c;
    }

    private static void fillCommon(RedisConfig c, Configuration config) {
        // COMMAND / ONERROR 为 e{} 枚举：RDK 返回 0-based 索引，故按 索引→值 映射；
        // 同时兼容直接返回标签(label)的情况，避免 RDK 版本差异导致失效。
        c.command = resolveCommand(str(config, "COMMAND", "SET"));
        c.keyTemplate = str(config, "KEY", null);
        c.valueTemplate = str(config, "VALUE", null);
        c.hashField = str(config, "HASH_FIELD", null);
        c.channel = str(config, "CHANNEL", null);
        c.dbIndex = Math.max(0, Math.min(15, intv(config, "DB_INDEX", c.dbIndex)));
        c.ttl = Math.max(0, intv(config, "TTL", c.ttl));
        c.ttlTemplate = str(config, "TTL", c.ttlTemplate);
        c.usernameEnabled = "true".equalsIgnoreCase(str(config, "USERNAME_ENABLED", "false"));
        c.username = c.usernameEnabled ? str(config, "USERNAME", null) : null;
        c.resultProperty = str(config, "RESULT_PROPERTY", c.resultProperty);
        c.onError = resolveOnError(str(config, "ONERROR", "0"));
    }

    /** 与 COMMAND 枚举 e{SET;GET;HSET;LPUSH;PUBLISH;DEL;EXISTS} 顺序一致，索引/标签均可解析。*/
    private static final String[] COMMAND_NAMES = {"SET","GET","HSET","LPUSH","PUBLISH","DEL","EXISTS"};
    private static String resolveCommand(String raw) {
        if (raw == null) return "SET";
        String up = raw.trim().toUpperCase();
        for (String name : COMMAND_NAMES) {
            if (name.equals(up)) return name;
        }
        try {
            int idx = Integer.parseInt(up);
            if (idx >= 0 && idx < COMMAND_NAMES.length) return COMMAND_NAMES[idx];
        } catch (NumberFormatException ignored) {}
        return "SET";
    }
    private static OnError resolveOnError(String raw) {
        if (raw == null) return OnError.ERROR_QUEUE;
        String up = raw.trim();
        for (OnError oe : OnError.values()) {
            if (oe.name().equalsIgnoreCase(up)) return oe;
        }
        try {
            int idx = Integer.parseInt(up);
            if (idx >= 0 && idx < OnError.values().length) return OnError.values()[idx];
        } catch (NumberFormatException ignored) {}
        return OnError.ERROR_QUEUE;
    }

    private static String str(Configuration config, String name, String dflt) {
        try {
            String v = config.get(name);
            return (v == null || v.isEmpty()) ? dflt : v;
        } catch (Exception e) {
            return dflt;
        }
    }

    private static int intv(Configuration config, String name, int dflt) {
        try {
            String v = config.get(name);
            return (v == null || v.isEmpty()) ? dflt : Integer.parseInt(v.trim());
        } catch (Exception e) {
            return dflt;
        }
    }

    // ---- getters ----
    public Mode getMode() { return mode; }
    public String getHost() { return host; }
    public int getPort() { return port; }
    public String getPassword() { return password; }
    public int getTimeout() { return timeout; }
    public String getMasterName() { return masterName; }
    public String getSentinelHosts() { return sentinelHosts; }
    public String getSentinelPassword() { return sentinelPassword; }
    public int getSentinelPort() { return sentinelPort; }
    public String getCommand() { return command; }
    public String getKeyTemplate() { return keyTemplate; }
    public String getValueTemplate() { return valueTemplate; }
    public String getHashField() { return hashField; }
    public String getChannel() { return channel; }
    public String getResultProperty() { return resultProperty; }
    public OnError getOnError() { return onError; }
    public int getDbIndex() { return dbIndex; }
    public int getTtl() { return ttl; }
    public String getTtlTemplate() { return ttlTemplate; }
    public boolean isUsernameEnabled() { return usernameEnabled; }
    public String getUsername() { return username; }
}
