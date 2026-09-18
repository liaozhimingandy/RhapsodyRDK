package com.alsoapp.rhapsody.rdk.commpoint.httprestful;

import com.orchestral.rhapsody.logging.Logger;
import com.orchestral.rhapsody.module.Configuration;
import com.orchestral.rhapsody.module.communicationpoint.AbstractCommunicationPoint;
import com.orchestral.rhapsody.module.communicationpoint.CommunicationPointConfiguration;
import com.orchestral.rhapsody.module.communicationpoint.CommunicationPointConnection;
import com.orchestral.rhapsody.module.communicationpoint.CommunicationPointException;
import com.orchestral.rhapsody.module.communicationpoint.CommunicationPointInfo;
import com.orchestral.rhapsody.module.communicationpoint.ConnectionException;
import com.orchestral.rhapsody.module.communicationpoint.SupportedMode;

import java.util.Locale;

/**
 * 自定义 HTTP 客户端通讯点（Out-&gt;In）。
 *
 * <p>用途：替代 Rhapsody 内置 HTTP 客户端，专门解决「请求 HTTPS 时因 JDK 默认
 * 启用的加密套件过少导致握手失败，而 Postman 能调通」的问题。本通讯点通过
 * {@link HttpRestfulSSLSocketFactory} 放开全部受支持的加密套件，并可显式选择
 * TLS 协议版本、跳过证书校验（自签名/测试）。
 *
 * <p>工作模式为 {@link SupportedMode#TWO_WAY}（配置为 Out-&gt;In）：
 * 路由中的消息体作为 HTTP 请求体发出，接口响应体作为下一条消息返回，
 * 响应状态码/响应头写入消息属性，便于下游分支处理。
 */
public class HttpRestfulCommPoint extends AbstractCommunicationPoint {

    public HttpRestfulCommPoint(final CommunicationPointInfo commPointInfo) {
        super(commPointInfo);
        this.config = new HttpRestfulConfig();
    }

    private static final String CLASS_NAME = HttpRestfulCommPoint.class.getSimpleName();

    // 注意：每条配置必须严格 6 段：
    // Name|Type|DefaultValue|Display Name|Dependency|Description
    // 缺段会在引擎注册通讯点类型时抛 ArrayIndexOutOfBoundsException，导致管理模块无法激活（登录不上）。

    // ========== 基础 ==========
    // URL / METHOD / CONTENT_TYPE / HEADERS 均支持动态取值：在值里写 ${propName} / $propName
    // 或 ${body} / $body，发送时按当前消息的属性/消息体实时解析（参考 redis 过滤器的写法）。
    // 不含 ${} / $ 的写法保持原样（静态值仍可用），与内置 HTTP 客户端行为一致。
    private static final String URL = "URL|*s||请求地址(URL)||目标接口地址，支持 ${prop}/$prop 与 ${body} 动态取值，如 https://api.example.com/${Path}";
    private static final String METHOD = "METHOD|e{GET;POST;PUT;DELETE;PATCH;HEAD;OPTIONS}|1|HTTP方法||下拉选择请求方式，默认 POST";
    private static final String CONTENT_TYPE = "CONTENT_TYPE|s|application/json|请求内容类型||发送体的Content-Type，支持 ${prop} 动态取值";
    private static final String HEADERS = "HEADERS|s||自定义请求头||每行 Name: Value，支持 ${prop} 动态取值";
    private static final String TIMEOUT = "TIMEOUT|*i|30000|超时时间(ms)||连接与读取超时，0=无限等待";

    // ========== SSL / 加密套件（本通讯点核心价值）============
    private static final String ENABLE_ALL_CIPHERS = "ENABLE_ALL_CIPHERS|z|true|启用全部加密套件||放开JDK默认禁用的套件，解决握手失败（核心）";
    private static final String TLS_PROTOCOLS = "TLS_PROTOCOLS|s|TLSv1.2,TLSv1.3|启用的TLS协议||逗号分隔；ALL=不限制";
    private static final String TRUST_ALL = "TRUST_ALL|z|false|忽略证书校验||跳过CA校验，仅自签名/测试用，生产慎用";

    // ========== 响应处理 ==========
    private static final String RESP_HEADER_PREFIX = "RESP_HEADER_PREFIX|s||响应头属性前缀||非空时把响应头逐条写入消息属性，如 http_r_";
    private static final String PROP_STATUS = "PROP_STATUS|s|http_status|状态码属性名||响应HTTP状态码写入的消息属性名";

    private static final String[] props = {
            URL, METHOD, CONTENT_TYPE, HEADERS, TIMEOUT,
            ENABLE_ALL_CIPHERS, TLS_PROTOCOLS, TRUST_ALL,
            RESP_HEADER_PREFIX, PROP_STATUS
    };

    private HttpRestfulConfig config;
    private Logger logger;

    @Override
    public String[] getPropertyList() {
        return props;
    }

    @Override
    public void configure(final CommunicationPointConfiguration cfg)
            throws CommunicationPointException, InterruptedException {
        final Configuration cfgProps = cfg.getConfiguratedProperties();

        final String url = cfgProps.get("URL");
        if (url == null || url.isBlank()) {
            throw new CommunicationPointException("请求地址(URL)不能为空");
        }
        config.setUrl(url.trim());

        config.setMethod(resolveMethod(cfgProps.get("METHOD", "1")));

        config.setContentType(cfgProps.get("CONTENT_TYPE", "application/json"));
        config.setHeaders(cfgProps.get("HEADERS"));

        try {
            config.setTimeout(cfgProps.getAsInt("TIMEOUT", 30000));
        } catch (final NumberFormatException e) {
            throw new CommunicationPointException("超时时间必须是整数(ms)", e);
        }

        config.setEnableAllCiphers("true".equalsIgnoreCase(cfgProps.get("ENABLE_ALL_CIPHERS", "true")));
        config.setTlsProtocols(cfgProps.get("TLS_PROTOCOLS", "TLSv1.2,TLSv1.3"));
        config.setTrustAll("true".equalsIgnoreCase(cfgProps.get("TRUST_ALL", "false")));

        config.setRespHeaderPrefix(cfgProps.get("RESP_HEADER_PREFIX"));
        config.setPropStatus(cfgProps.get("PROP_STATUS", "http_status"));

        this.logger = getLogger();
    }

    @Override
    public CommunicationPointConnection getConnection() throws ConnectionException, InterruptedException {
        return new HttpRestfulConnection(config, getLogger());
    }

    /**
     * METHOD 属性已改为下拉枚举（e{GET;POST;PUT;DELETE;PATCH;HEAD;OPTIONS}），
     * IDE 存的是从 0 开始的整数索引；这里把索引映射回 HTTP 方法名。
     * 同时兼容直接写入方法名字符串（旧配置/手填），并对非法值报错，避免输错方法名。
     */
    private static String resolveMethod(String raw) throws CommunicationPointException {
        final String[] NAMES = {"GET", "POST", "PUT", "DELETE", "PATCH", "HEAD", "OPTIONS"};
        if (raw == null || raw.isBlank()) {
            return "POST";
        }
        final String v = raw.trim();
        try {
            final int idx = Integer.parseInt(v);
            if (idx < 0 || idx >= NAMES.length) {
                throw new CommunicationPointException(
                        "HTTP方法下拉索引越界: " + idx + "（合法范围 0-" + (NAMES.length - 1) + "）");
            }
            return NAMES[idx];
        } catch (final NumberFormatException notAnIndex) {
            final String m = v.toUpperCase(Locale.ROOT);
            for (final String name : NAMES) {
                if (name.equals(m)) {
                    return m;
                }
            }
            throw new CommunicationPointException(
                    "不支持的HTTP方法: " + v + "（可选: GET/POST/PUT/DELETE/PATCH/HEAD/OPTIONS）");
        }
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
