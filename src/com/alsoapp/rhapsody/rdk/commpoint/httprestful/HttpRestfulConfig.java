package com.alsoapp.rhapsody.rdk.commpoint.httprestful;

/**
 * HttpRestful 通讯点配置。
 *
 * <p>对应 {@link HttpRestfulCommPoint#getPropertyList()} 暴露的属性。
 * 核心目的：解决 Rhapsody 内置 HTTP 客户端请求 HTTPS 时因 JDK 默认启用的
 * 加密套件过少而握手失败的问题 —— 通过 {@link HttpRestfulSSLSocketFactory}
 * 放开全部受支持的加密套件，并可显式选择 TLS 协议版本。
 */
public class HttpRestfulConfig {

    // ========== 基础 ==========
    private String url;
    private String method = "POST";
    private String contentType = "application/json";
    private String headers;          // 多行 "Name: Value"

    // ========== 超时 ==========
    private int timeout = 30000;     // 连接 + 读取超时(ms)，0=无限

    // ========== SSL / 加密套件（本通讯点核心价值）============
    /** 启用全部受支持的加密套件，放开 JDK 默认禁用的套件 */
    private boolean enableAllCiphers = true;
    /** 启用的 TLS 协议，逗号分隔；为空或 ALL 表示不限制（使用 JDK 默认） */
    private String tlsProtocols = "TLSv1.2,TLSv1.3";
    /** 跳过 CA 校验（自签名 / 测试环境用，生产慎用） */
    private boolean trustAll = false;

    // ========== 响应处理 ==========
    /** 非空时把响应头逐条写入消息属性，如 "http_r_" */
    private String respHeaderPrefix;
    /** 响应状态码写入的消息属性名 */
    private String propStatus = "http_status";

    // ========== getter / setter ==========
    public String getUrl() { return url; }
    public void setUrl(String url) { this.url = url; }

    public String getMethod() { return method; }
    public void setMethod(String method) { this.method = method; }

    public String getContentType() { return contentType; }
    public void setContentType(String contentType) { this.contentType = contentType; }

    public String getHeaders() { return headers; }
    public void setHeaders(String headers) { this.headers = headers; }

    public int getTimeout() { return timeout; }
    public void setTimeout(int timeout) { this.timeout = timeout; }

    public boolean isEnableAllCiphers() { return enableAllCiphers; }
    public void setEnableAllCiphers(boolean enableAllCiphers) { this.enableAllCiphers = enableAllCiphers; }

    public String getTlsProtocols() { return tlsProtocols; }
    public void setTlsProtocols(String tlsProtocols) { this.tlsProtocols = tlsProtocols; }

    public boolean isTrustAll() { return trustAll; }
    public void setTrustAll(boolean trustAll) { this.trustAll = trustAll; }

    public String getRespHeaderPrefix() { return respHeaderPrefix; }
    public void setRespHeaderPrefix(String respHeaderPrefix) { this.respHeaderPrefix = respHeaderPrefix; }

    public String getPropStatus() { return propStatus; }
    public void setPropStatus(String propStatus) { this.propStatus = propStatus; }
}
