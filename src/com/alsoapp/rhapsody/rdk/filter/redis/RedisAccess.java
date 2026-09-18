package com.alsoapp.rhapsody.rdk.filter.redis;

import java.util.List;

import com.orchestral.rhapsody.logging.Logger;
import com.orchestral.rhapsody.message.Message;

import redis.clients.jedis.DefaultJedisClientConfig;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisClientConfig;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

/**
 * 对 Jedis 的薄封装：根据 RedisConfig 建立连接池，并提供 execute() 执行命令。
 * - CLUSTER 模式：Predixy 已把集群收敛为单端点，按普通单机连接（host/port/password）。
 * - SENTINEL 模式：显式连哨兵（带 AUTH）发现 master 真实地址，再用普通 JedisPool 直连该地址。
 *   说明：标准 JedisSentinelPool 在 Jedis 4.4.6 对“哨兵密码”的传递有版本歧义，且会直连发现的真实
 *   内网 IP；此处改为手动发现 + 普通池，完全复刻 redis-cli 已验证可用的路径：
 *     redis-cli -h service.lhim.com -p 36379 -a lhim.org SENTINEL get-master-addr-by-name lhsentinel
 *     -> 10.254.208.48:26379（引擎与内网 Redis 同网段，直连可达，AUTH 同密码 lhim.org）。
 *   注意：master 在 connect() 时一次性发现，不做运行时故障自动转移（如需 failover 需扩展）。
 */
public class RedisAccess {

    private final RedisConfig cfg;
    private final Logger logger;

    private JedisPool pool;

    public RedisAccess(RedisConfig cfg, Logger logger) {
        this.cfg = cfg;
        this.logger = logger;
        connect();
    }

    private void connect() {
        if (cfg.getMode() == RedisConfig.Mode.SENTINEL) {
            if (cfg.getMasterName() == null || cfg.getMasterName().isEmpty()
                    || cfg.getSentinelHosts() == null || cfg.getSentinelHosts().isEmpty()) {
                throw new IllegalArgumentException("SENTINEL 模式必须配置 MASTER_NAME 与 SENTINEL_HOSTS");
            }
            String[] master = discoverMaster();
            String mHost = master[0];
            int mPort = Integer.parseInt(master[1]);
            JedisPoolConfig pc = new JedisPoolConfig();
            pc.setMaxTotal(16);
            pc.setMaxIdle(8);
            // 引擎与内网 Redis 同网段，可直连哨兵发现的 master 真实内网地址（已 redis-cli 验证可达）。
            // 数据节点鉴权：密码沿用 SENTINEL_PASSWORD（与哨兵相同）；若启用 ACL 则带用户名。
            // database=cfg.getDbIndex()：Jedis 在连接建立时发送 SELECT（db≠0），db0 为默认库。
            this.pool = newPool(pc, mHost, mPort, cfg.getSentinelPassword());
            if (logger != null) logger.info("Redis sentinel: discovered master=" + mHost + ":" + mPort
                    + " db=" + cfg.getDbIndex() + " -> pool created");
        } else {
            JedisPoolConfig pc = new JedisPoolConfig();
            pc.setMaxTotal(16);
            pc.setMaxIdle(8);
            this.pool = newPool(pc, cfg.getHost(), cfg.getPort(), cfg.getPassword());
            if (logger != null) logger.info("Redis pool created -> " + cfg.getHost() + ":" + cfg.getPort()
                    + " db=" + cfg.getDbIndex());
        }
    }

    /**
     * 遍历 SENTINEL_HOSTS（逗号分隔），用哨兵密码 AUTH 连上后执行
     * SENTINEL get-master-addr-by-name 发现 master 的 [host, port]。
     * 任一个哨兵成功即返回；全部失败抛出 IllegalStateException。
     */
    private String[] discoverMaster() {
        String pwd = orEmpty(cfg.getSentinelPassword());
        for (String raw : cfg.getSentinelHosts().split("[,;]")) {
            String t = raw.trim();
            if (t.isEmpty()) continue;
            String[] hp = t.split(":", 2);
            String sHost = hp[0];
            int sPort;
            if (hp.length >= 2) {
                try {
                    sPort = Integer.parseInt(hp[1].trim());
                } catch (NumberFormatException e) {
                    sPort = cfg.getSentinelPort(); // host:port 端口非法时回退到独立 PORT 字段
                }
            } else {
                sPort = cfg.getSentinelPort(); // 新模型：host 无端口，端口取自 PORT 字段
            }
            try {
                JedisClientConfig cc = buildClientConfig(pwd);
                try (Jedis sentinel = new Jedis(sHost, sPort, cc)) {
                    List<String> addr = sentinel.sentinelGetMasterAddrByName(cfg.getMasterName());
                    if (addr != null && addr.size() >= 2) {
                        return new String[]{ addr.get(0), addr.get(1) };
                    }
                    if (logger != null) logger.warn("哨兵 " + sHost + ":" + sPort + " 未返回 master 地址");
                }
            } catch (Exception e) {
                if (logger != null) logger.warn("哨兵 " + sHost + ":" + sPort + " 发现 master 失败: " + e.getMessage());
            }
        }
        throw new IllegalStateException("所有哨兵均无法发现 master " + cfg.getMasterName());
    }

    private static String orEmpty(String s) {
        return (s == null) ? null : s;
    }

    /** 根据是否配置 ACL 用户名选择 JedisPool 构造：带用户用 7 参(user,password,database)，否则 6 参(password,database)。*/
    private JedisPool newPool(JedisPoolConfig pc, String host, int port, String password) {
        String user = cfg.getUsername();
        if (user != null) {
            return new JedisPool(pc, host, port, cfg.getTimeout(), user, password, cfg.getDbIndex());
        }
        return new JedisPool(pc, host, port, cfg.getTimeout(), password, cfg.getDbIndex());
    }

    /** 构造哨兵/数据节点连接用的 JedisClientConfig，启用 ACL 时带上用户名。*/
    private JedisClientConfig buildClientConfig(String password) {
        DefaultJedisClientConfig.Builder b = DefaultJedisClientConfig.builder()
                .password(password)
                .timeoutMillis(cfg.getTimeout());
        String user = cfg.getUsername();
        if (user != null) {
            b.user(user);
        }
        return b.build();
    }

    /** TTL>0 时对 key 设置过期时间（秒）。仅对写类命令生效。TTL 可为静态数字或消息属性模板(${prop}/$prop)，按每条消息解析。*/
    private void applyTtl(Jedis j, String key, Message msg) {
        int ttl = resolveTtl(msg);
        if (ttl > 0) {
            j.expire(key, ttl);
        }
    }

    /** 解析 TTL：模板经 TemplateResolver 按消息属性求值后转 int；缺失/非数字时回退到静态 ttl。*/
    private int resolveTtl(Message msg) {
        String raw = cfg.getTtlTemplate();
        if (raw == null || raw.trim().isEmpty()) {
            return cfg.getTtl();
        }
        String resolved = TemplateResolver.resolve(raw, msg);
        if (resolved == null || resolved.trim().isEmpty()) {
            return cfg.getTtl();
        }
        try {
            return Math.max(0, Integer.parseInt(resolved.trim()));
        } catch (NumberFormatException e) {
            return cfg.getTtl();
        }
    }

    private Jedis getResource() {
        return pool.getResource();
    }

    /**
     * 执行一条命令，返回结果字符串（无结果返回 null）。
     * 命令名取自 cfg.getCommand()，key/value/field/channel 为已解析的实际值。
     */
    public String execute(String command, String key, String value, String field, String channel, Message msg) {
        try (Jedis j = getResource()) {
            String result;
            switch (command.toUpperCase()) {
                case "SET":     result = j.set(key, value); applyTtl(j, key, msg); return result;
                case "GET":     return j.get(key);
                case "SETEX": {
                    long secs = Long.parseLong(value.split("\\s+", 2)[0]);
                    // SETEX 自带过期，不再叠加 TTL
                    return j.setex(key, secs, value.substring(value.indexOf(' ') + 1));
                }
                case "APPEND":  result = String.valueOf(j.append(key, value)); applyTtl(j, key, msg); return result;
                case "DEL":     return String.valueOf(j.del(key));
                case "EXISTS":  return String.valueOf(j.exists(key));
                case "INCR":    result = String.valueOf(j.incr(key)); applyTtl(j, key, msg); return result;
                case "HSET":    result = String.valueOf(j.hset(key, field, value)); applyTtl(j, key, msg); return result;
                case "HGET":    return j.hget(key, field);
                case "HMSET":   return "OK"; // 复杂结构请用 Lua/脚本，此处占位
                case "LPUSH":   result = String.valueOf(j.lpush(key, value)); applyTtl(j, key, msg); return result;
                case "RPUSH":   result = String.valueOf(j.rpush(key, value)); applyTtl(j, key, msg); return result;
                case "PUBLISH": return String.valueOf(j.publish(channel, value));
                default:
                    throw new IllegalArgumentException("不支持的 Redis 命令: " + command);
            }
        }
    }

    public void close() {
        try { if (pool != null) pool.close(); } catch (Exception e) { /* ignore */ }
    }
}
