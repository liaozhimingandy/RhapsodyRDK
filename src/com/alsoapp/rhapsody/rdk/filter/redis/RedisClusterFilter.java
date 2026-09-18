package com.alsoapp.rhapsody.rdk.filter.redis;

import com.orchestral.rhapsody.logging.Logger;

import com.orchestral.rhapsody.message.Message;
import com.orchestral.rhapsody.message.MessageException;
import com.orchestral.rhapsody.message.Messages;
import com.orchestral.rhapsody.module.Configuration;
import com.orchestral.rhapsody.module.filter.AbstractFilter;
import com.orchestral.rhapsody.module.filter.Filter;
import com.orchestral.rhapsody.module.filter.FilterConfigurationException;
import com.orchestral.rhapsody.module.filter.FilterInfo;

/**
 * Redis Cluster 过滤器（经 Predixy 代理入口，当作单端点连接）。
 * 连接 Predixy: 10.254.147.60:7617, 前端密码 cdr@lhim.org。
 */
public class RedisClusterFilter extends AbstractFilter {

    // ---- IDE 配置项（竖线 6 字段: Name|Type|Default|Display|Dependency|Description）----
    private static final String HOST     = "HOST|s|10.254.147.60|Redis Host (Predixy)||Predixy 代理入口";
    private static final String PORT     = "PORT|iP|7617|Redis Port||Predixy 代理端口";
    private static final String PASSWORD = "PASSWORD|w|cdr@lhim.org|Password||Predixy 前端密码";
    private static final String USERNAME_ENABLED   = "USERNAME_ENABLED|z|false|Enable Username||勾选后启用 Redis ACL 用户名";
    private static final String USERNAME           = "USERNAME|s||Username|USERNAME_ENABLED=true|Redis ACL 用户名(ACL)";
    private static final String TIMEOUT  = "TIMEOUT|i|2000|Timeout(ms)||连接超时";
    private static final String DB_INDEX = "DB_INDEX|i|0|Database Index||Redis 库编号 0-15, 默认 0(db0)";
    private static final String TTL      = "TTL|s|0|TTL(s)||过期时间(秒), 0=不过期; 支持 ${propName}/$propName 动态取值";
    private static final String COMMAND  = "COMMAND|e{SET;GET;HSET;LPUSH;PUBLISH;DEL;EXISTS}|SET|Redis Command||选择要执行的 Redis 命令";
    private static final String KEY      = "KEY|*s||Key||支持 ${propName}/$propName 与 ${body}/$body";
    private static final String VALUE    = "VALUE|s||Value||写命令的值, 支持 ${propName}/$propName 与 ${body}/$body";
    private static final String HASH_FIELD = "HASH_FIELD|s||Hash Field||HSET/HGET 字段名, 支持 ${propName}/$propName";
    private static final String CHANNEL  = "CHANNEL|s||Channel||PUBLISH 频道, 支持 ${propName}/$propName";
    private static final String RESULT   = "RESULT_PROPERTY|s|RedisResult|Result Property||结果写入的消息属性名";
    private static final String ONERROR  = "ONERROR|e{Error Queue; Ignore; Exception}|Error Queue|On Redis Error||执行失败时的处理";

    private static final String[] PROPS = {
        HOST, PORT, PASSWORD, USERNAME_ENABLED, USERNAME, TIMEOUT, DB_INDEX, TTL, COMMAND,
        KEY, VALUE, HASH_FIELD, CHANNEL, RESULT, ONERROR
    };

    private RedisConfig config;
    private RedisAccess access;
    private final Logger logger = getLogger();

    public RedisClusterFilter(FilterInfo filterInfo) {
        super(filterInfo);
    }

    @Override
    public String[] getPropertyList() {
        return PROPS;
    }

    @Override
    public void doConfigure(Configuration configuration) throws FilterConfigurationException, InterruptedException {
        // 若已配置过，先释放旧连接
        if (this.access != null) {
            this.access.close();
            this.access = null;
        }
        this.config = RedisConfig.fromClusterConfig(configuration);
        if (this.config.getKeyTemplate() == null || this.config.getKeyTemplate().isEmpty()) {
            throw new FilterConfigurationException("Required field 'KEY' missing.");
        }
        try {
            this.access = new RedisAccess(this.config, logger);
        } catch (Exception e) {
            throw new FilterConfigurationException("Redis 连接失败: " + e.getMessage());
        }
    }

    @Override
    public Message[] doProcessMessage(Message[] messages)
            throws MessageException, com.orchestral.rhapsody.module.filter.FilterProcessingException {
        for (int i = 0; i < messages.length; i++) {
            Message msg = messages[i];
            try {
                String key = TemplateResolver.resolve(config.getKeyTemplate(), msg);
                String value = TemplateResolver.resolve(config.getValueTemplate(), msg);
                String field = TemplateResolver.resolve(config.getHashField(), msg);
                String channel = TemplateResolver.resolve(config.getChannel(), msg);

                String result = access.execute(config.getCommand(), key, value, field, channel, msg);

                if (config.getResultProperty() != null && !config.getResultProperty().isEmpty()) {
                    try {
                        msg.getProperty(config.getResultProperty()).setValue(result == null ? "" : result);
                    } catch (Exception e) {
                        logger.warn("写入结果属性失败: " + e.getMessage());
                    }
                }
                if (logger.isDebugEnabled()) {
                    logger.debug("Redis " + config.getCommand() + " key=" + key + " -> " + result);
                }
            } catch (Exception e) {
                handleError(msg, e);
            }
        }
        return messages;
    }

    private void handleError(Message msg, Exception e)
            throws MessageException, com.orchestral.rhapsody.module.filter.FilterProcessingException {
        logger.error("Redis 操作失败: " + e.getMessage(), e);
        switch (config.getOnError()) {
            case IGNORE:
                break;
            case EXCEPTION:
                throw new MessageException("Redis 操作失败: " + e.getMessage(), e);
            case ERROR_QUEUE:
            default:
                throw new com.orchestral.rhapsody.module.filter.FilterProcessingException(
                        "Redis 操作失败: " + e.getMessage(), e);
        }
    }

    @Override
    public Filter.FilterKind getKind() {
        // DB 类过滤器建议 LIMITED_CONCURRENCY，避免连接池被并发打爆
        return Filter.FilterKind.LIMITED_CONCURRENCY;
    }

    /** 组件被移除/停用时释放连接 */
    @Override
    public void delete() throws com.orchestral.rhapsody.module.filter.FilterException {
        if (this.access != null) {
            this.access.close();
            this.access = null;
        }
        super.delete();
    }
}
