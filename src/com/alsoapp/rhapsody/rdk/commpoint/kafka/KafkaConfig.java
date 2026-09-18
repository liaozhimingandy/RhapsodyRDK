package com.alsoapp.rhapsody.rdk.commpoint.kafka;

/**
 * Kafka 通讯点配置，只保留常用项共 11 个。
 *
 * <p>精简说明：原先暴露的调优类参数（acks / retries / batch.size / linger.ms /
 * compression.type / auto.offset.reset / enable.auto.commit / session.timeout.ms /
 * max.poll.records）与「消息 Key 策略」「Headers 透传」已从界面移除，
 * 并固化为默认值常量，集中定义在 {@link KafkaConnection} 顶部，需要调整直接改那里。
 * 固化的取值与精简前完全一致，因此行为不变。
 *
 * <p>其中 {@code maxRequestSize}（max.request.size）是本通讯点的核心价值：
 * 引擎自带的 Kafka 通讯点未开放此参数，超过 1MB 的消息会被 Broker 拒收。
 */
public class KafkaConfig {

    // ========== 基础 ==========
    private String bootstrapServers;
    private String topic;
    private String groupId;

    // ========== 消息大小（控制收发字节数）==========
    /** max.request.size，生产者单条消息最大字节 */
    private int maxRequestSize;
    /** max.partition.fetch.bytes，消费者单分区单次拉取上限 */
    private int maxPartitionFetchBytes;
    /** fetch.max.bytes，消费者单次拉取总字节上限 */
    private int fetchMaxBytes;

    // ========== SASL / SSL ==========
    private boolean enableSasl;
    private String saslMechanism;
    private String saslUsername;
    private String saslPassword;
    private boolean enableSsl;

    // ========== getter / setter ==========
    public String getBootstrapServers() { return bootstrapServers; }
    public void setBootstrapServers(String bootstrapServers) { this.bootstrapServers = bootstrapServers; }

    public String getTopic() { return topic; }
    public void setTopic(String topic) { this.topic = topic; }

    public String getGroupId() { return groupId; }
    public void setGroupId(String groupId) { this.groupId = groupId; }

    public int getMaxRequestSize() { return maxRequestSize; }
    public void setMaxRequestSize(int maxRequestSize) { this.maxRequestSize = maxRequestSize; }

    public int getMaxPartitionFetchBytes() { return maxPartitionFetchBytes; }
    public void setMaxPartitionFetchBytes(int maxPartitionFetchBytes) { this.maxPartitionFetchBytes = maxPartitionFetchBytes; }

    public int getFetchMaxBytes() { return fetchMaxBytes; }
    public void setFetchMaxBytes(int fetchMaxBytes) { this.fetchMaxBytes = fetchMaxBytes; }

    public boolean isEnableSasl() { return enableSasl; }
    public void setEnableSasl(boolean enableSasl) { this.enableSasl = enableSasl; }

    public String getSaslMechanism() { return saslMechanism; }
    public void setSaslMechanism(String saslMechanism) { this.saslMechanism = saslMechanism; }

    public String getSaslUsername() { return saslUsername; }
    public void setSaslUsername(String saslUsername) { this.saslUsername = saslUsername; }

    public String getSaslPassword() { return saslPassword; }
    public void setSaslPassword(String saslPassword) { this.saslPassword = saslPassword; }

    public boolean isEnableSsl() { return enableSsl; }
    public void setEnableSsl(boolean enableSsl) { this.enableSsl = enableSsl; }
}
