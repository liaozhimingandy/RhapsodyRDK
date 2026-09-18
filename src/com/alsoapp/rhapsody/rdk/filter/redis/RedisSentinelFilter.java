package com.alsoapp.rhapsody.rdk.filter.redis;

import com.orchestral.rhapsody.logging.Logger;

import com.orchestral.rhapsody.message.Message;
import com.orchestral.rhapsody.message.MessageException;
import com.orchestral.rhapsody.module.Configuration;
import com.orchestral.rhapsody.module.filter.AbstractFilter;
import com.orchestral.rhapsody.module.filter.Filter;
import com.orchestral.rhapsody.module.filter.FilterConfigurationException;
import com.orchestral.rhapsody.module.filter.FilterInfo;
import com.orchestral.rhapsody.module.filter.FilterProcessingException;

/**
 * Redis Sentinel 过滤器（哨兵模式）。
 * 通过 masterName + 哨兵列表发现 master，再执行命令。
 */
public class RedisSentinelFilter extends AbstractFilter {

    private static final String MASTER_NAME       = "MASTER_NAME|*s||Sentinel Master Name||哨兵 master 名称, 如 lhsentinel";
    private static final String SENTINEL_HOSTS     = "SENTINEL_HOSTS|*s||Sentinel Hosts||逗号分隔 host, 端口见下方 PORT 字段, 如 service.lhim.com";
    private static final String PORT               = "PORT|iP|36379|Sentinel Port||哨兵端口(独立配置)";
    private static final String SENTINEL_PASSWORD  = "SENTINEL_PASSWORD|w||Sentinel Password||哨兵/数据节点鉴权(可选)";
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
        MASTER_NAME, SENTINEL_HOSTS, PORT, SENTINEL_PASSWORD, USERNAME_ENABLED, USERNAME,
        TIMEOUT, DB_INDEX, TTL, COMMAND,
        KEY, VALUE, HASH_FIELD, CHANNEL, RESULT, ONERROR
    };

    private RedisConfig config;
    private RedisAccess access;
    private final Logger logger = getLogger();

    public RedisSentinelFilter(FilterInfo filterInfo) {
        super(filterInfo);
    }

    @Override
    public String[] getPropertyList() {
        return PROPS;
    }

    @Override
    public void doConfigure(Configuration configuration) throws FilterConfigurationException, InterruptedException {
        if (this.access != null) {
            this.access.close();
            this.access = null;
        }
        this.config = RedisConfig.fromSentinelConfig(configuration);
        if (this.config.getKeyTemplate() == null || this.config.getKeyTemplate().isEmpty()) {
            throw new FilterConfigurationException("Required field 'KEY' missing.");
        }
        try {
            this.access = new RedisAccess(this.config, logger);
        } catch (Exception e) {
            throw new FilterConfigurationException("Redis Sentinel 连接失败: " + e.getMessage());
        }
    }

    @Override
    public Message[] doProcessMessage(Message[] messages) throws MessageException, FilterProcessingException {
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

    private void handleError(Message msg, Exception e) throws MessageException, FilterProcessingException {
        logger.error("Redis 操作失败: " + e.getMessage(), e);
        switch (config.getOnError()) {
            case IGNORE:
                break;
            case EXCEPTION:
                throw new MessageException("Redis 操作失败: " + e.getMessage(), e);
            case ERROR_QUEUE:
            default:
                throw new FilterProcessingException("Redis 操作失败: " + e.getMessage(), e);
        }
    }

    @Override
    public Filter.FilterKind getKind() {
        return Filter.FilterKind.LIMITED_CONCURRENCY;
    }

    @Override
    public void delete() throws com.orchestral.rhapsody.module.filter.FilterException {
        if (this.access != null) {
            this.access.close();
            this.access = null;
        }
        super.delete();
    }
}
