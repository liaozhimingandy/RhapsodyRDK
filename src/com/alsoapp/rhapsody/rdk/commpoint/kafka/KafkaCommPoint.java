package com.alsoapp.rhapsody.rdk.commpoint.kafka;

import com.orchestral.rhapsody.module.Configuration;
import com.orchestral.rhapsody.module.communicationpoint.AbstractCommunicationPoint;
import com.orchestral.rhapsody.module.communicationpoint.CommunicationPointConfiguration;
import com.orchestral.rhapsody.module.communicationpoint.CommunicationPointConnection;
import com.orchestral.rhapsody.module.communicationpoint.CommunicationPointException;
import com.orchestral.rhapsody.module.communicationpoint.CommunicationPointInfo;
import com.orchestral.rhapsody.module.communicationpoint.ConnectionException;
import com.orchestral.rhapsody.module.communicationpoint.SupportedMode;


public class KafkaCommPoint extends AbstractCommunicationPoint {

    public KafkaCommPoint(CommunicationPointInfo commPointInfo) {
        super(commPointInfo);
        this.config = new KafkaConfig();
    }

    private static final String CLASS_NAME = KafkaCommPoint.class.getSimpleName();

    // 注意：每条配置必须严格 6 段：
    // Name|Type|DefaultValue|Display Name|Dependency|Description
    // 缺段会在引擎注册通讯点类型时抛 ArrayIndexOutOfBoundsException，导致管理模块无法激活（登录不上）。

    // ========== 基础配置 ==========
    private static final String BOOTSTRAP = "BOOTSTRAP_SERVERS|*s||Kafka集群地址||host1:9092,host2:9092";
    private static final String TOPIC = "TOPIC|*s||Topic名称||读写的topic名称";
    private static final String GROUP_ID = "GROUP_ID|s||消费组ID||输入模式必填，如 kafka-cp-group";

    // ========== 消息大小（本通讯点核心：控制收发字节数）==========
    private static final String MAX_REQUEST_SIZE = "MAX_REQUEST_SIZE|*i|1048576|单消息最大字节||max.request.size，超过会被Broker拒收，大消息按需调大";
    private static final String MAX_PARTITION_FETCH = "MAX_PARTITION_FETCH|*i|1048576|单分区拉取上限||max.partition.fetch.bytes，需不小于单条消息大小";
    private static final String FETCH_MAX_BYTES = "FETCH_MAX_BYTES|*i|52428800|单次拉取总字节上限||fetch.max.bytes，需不小于单条消息大小";

    // ========== 安全 SASL/SSL ==========
    private static final String ENABLE_SASL = "ENABLE_SASL|z|false|启用SASL认证||";
    private static final String SASL_MECHANISM = "SASL_MECHANISM|s|PLAIN|SASL机制||PLAIN / SCRAM-SHA-256 等";
    private static final String SASL_USER = "SASL_USER|s||SASL用户名||";
    private static final String SASL_PWD = "SASL_PWD|w||SASL密码||IDE中显示为******";
    private static final String ENABLE_SSL = "ENABLE_SSL|z|false|启用SSL传输||";

    // 共 11 项。原 acks / retries / 批处理 / 压缩 / offset / Key策略 / Headers 等
    // 已固化为 KafkaConnection 顶部的常量，不再暴露。
    private static final String[] props = {
            BOOTSTRAP, TOPIC, GROUP_ID,
            MAX_REQUEST_SIZE, MAX_PARTITION_FETCH, FETCH_MAX_BYTES,
            ENABLE_SASL, SASL_MECHANISM, SASL_USER, SASL_PWD, ENABLE_SSL
    };

    private KafkaConfig config;


    @Override
    public String[] getPropertyList() {
        return props;
    }

    @Override
    public void configure(CommunicationPointConfiguration cfg) throws CommunicationPointException, InterruptedException {
        // 获取通讯点原始Properties配置对象
        Configuration cfgProps = cfg.getConfiguratedProperties();

        // 读取必填参数
        String bootstrap = cfgProps.get("BOOTSTRAP_SERVERS");
        String topic = cfgProps.get("TOPIC");

        if (bootstrap == null || bootstrap.isBlank()) {
            throw new CommunicationPointException("Kafka集群地址不能为空");
        }
        if (topic == null || topic.isBlank()) {
            throw new CommunicationPointException("Topic不能为空");
        }

        config.setBootstrapServers(bootstrap.trim());
        config.setTopic(topic.trim());
        config.setGroupId(cfgProps.get("GROUP_ID"));

        // 数值配置，第二个参数为默认值，和 getPropertyList 里面定义保持一致
        try {
            config.setMaxRequestSize(Integer.parseInt(cfgProps.get("MAX_REQUEST_SIZE", "1048576")));
            config.setMaxPartitionFetchBytes(Integer.parseInt(cfgProps.get("MAX_PARTITION_FETCH", "1048576")));
            config.setFetchMaxBytes(Integer.parseInt(cfgProps.get("FETCH_MAX_BYTES", "52428800")));
        } catch (NumberFormatException e) {
            throw new CommunicationPointException("数值型配置项必须是整数", e);
        }

        // SASL / SSL
        config.setEnableSasl("true".equalsIgnoreCase(cfgProps.get("ENABLE_SASL", "false")));
        config.setSaslMechanism(cfgProps.get("SASL_MECHANISM", "PLAIN"));
        config.setSaslUsername(cfgProps.get("SASL_USER"));
        config.setSaslPassword(cfgProps.get("SASL_PWD"));
        config.setEnableSsl("true".equalsIgnoreCase(cfgProps.get("ENABLE_SSL", "false")));
    }


    @Override
    public CommunicationPointConnection getConnection() throws ConnectionException, InterruptedException {
        return new KafkaConnection(config);
    }

    @Override
    public SupportedMode getImplementationKind() {
        return SupportedMode.TWO_WAY;
    }

    @Override
    public void shutdown() {
        // 无全局资源
    }

}
