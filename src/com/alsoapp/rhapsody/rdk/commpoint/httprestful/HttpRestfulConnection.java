package com.alsoapp.rhapsody.rdk.commpoint.httprestful;

import com.orchestral.rhapsody.logging.Level;
import com.orchestral.rhapsody.logging.Logger;
import com.orchestral.rhapsody.message.Message;
import com.orchestral.rhapsody.message.Messages;
import com.orchestral.rhapsody.message.MessageException;
import com.orchestral.rhapsody.module.communicationpoint.CommunicationPointConnection;
import com.orchestral.rhapsody.module.communicationpoint.ConnectionException;
import com.orchestral.rhapsody.module.communicationpoint.ConnectionFailedException;
import com.orchestral.rhapsody.module.communicationpoint.SendMessageFailedException;

import javax.net.ssl.HostnameVerifier;
import javax.net.ssl.HttpsURLConnection;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLSocketFactory;
import javax.net.ssl.TrustManager;
import javax.net.ssl.X509TrustManager;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.ProtocolException;
import java.net.URL;
import java.security.SecureRandom;
import java.security.cert.X509Certificate;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * HttpRestful 通讯点连接（Out-&gt;In 模式）。
 *
 * <p>每次 {@code sendMessage} 用消息体作为 HTTP 请求体调用外部接口，
 * 把响应（状态码 / 响应头 / 响应体）暂存；引擎随后调用 {@code receiveMessage}
 * 把响应体写回下一条消息，从而实现「发送请求-等待响应」的闭环。
 *
 * <p>无长连接：每条消息独立打开一个 HttpURLConnection，因此连接对象本身
 * 不持有 socket，{@code dropConnection / reconnect} 只维护状态。
 */
public class HttpRestfulConnection implements CommunicationPointConnection {

    private static final AtomicInteger ID_GEN = new AtomicInteger();
    private static final String USER_AGENT = "RhapsodyRDK-HttpRestful/1.0";

    private final int id;
    private final HttpRestfulConfig config;
    private final Logger logger;

    private State state = State.CLOSED;
    private SSLSocketFactory sslSocketFactory;
    private HostnameVerifier hostnameVerifier;

    /** 最近一次 sendMessage 捕获的响应，receiveMessage 取回后清空 */
    private volatile PendingResponse pendingResponse;

    private static final class PendingResponse {
        final int statusCode;
        final String statusText;
        final String contentType;
        final Map<String, String> headers;
        final byte[] body;

        PendingResponse(final int statusCode, final String statusText, final String contentType,
                        final Map<String, String> headers, final byte[] body) {
            this.statusCode = statusCode;
            this.statusText = statusText;
            this.contentType = contentType;
            this.headers = headers;
            this.body = body;
        }
    }

    public HttpRestfulConnection(final HttpRestfulConfig config, final Logger logger)
            throws ConnectionFailedException {
        this.id = ID_GEN.getAndIncrement();
        this.config = config;
        this.logger = logger;
        prepareSsl();
        this.state = State.CONNECTED;
    }

    // ============ SSL 初始化 ============
    private void prepareSsl() throws ConnectionFailedException {
        try {
            final String[] protocols = parseProtocols(config.getTlsProtocols());
            final SSLContext ctx;
            if (config.isTrustAll()) {
                // 信任所有证书（自签名 / 测试环境）。生产环境请勿长期开启。
                ctx = SSLContext.getInstance("TLS");
                ctx.init(null, new TrustManager[]{new X509TrustManager() {
                    @Override
                    public void checkClientTrusted(final X509Certificate[] chain, final String authType) {
                    }

                    @Override
                    public void checkServerTrusted(final X509Certificate[] chain, final String authType) {
                    }

                    @Override
                    public X509Certificate[] getAcceptedIssuers() {
                        return new X509Certificate[0];
                    }
                }}, new SecureRandom());
                this.hostnameVerifier = (hostname, session) -> true;
            } else {
                // 使用 JDK 默认信任库（系统 CA），仅放开套件/协议
                ctx = SSLContext.getDefault();
                this.hostnameVerifier = HttpsURLConnection.getDefaultHostnameVerifier();
            }
            this.sslSocketFactory = new HttpRestfulSSLSocketFactory(ctx, protocols, config.isEnableAllCiphers());
        } catch (final Exception e) {
            throw new ConnectionFailedException("初始化SSL上下文失败:" + e.getMessage(), e);
        }
    }

    private static String[] parseProtocols(final String raw) {
        if (raw == null) {
            return null;
        }
        final String t = raw.trim();
        if (t.isEmpty() || "ALL".equalsIgnoreCase(t)) {
            return null;
        }
        return Arrays.stream(t.split(",")).map(String::trim).filter(s -> !s.isEmpty())
                .toArray(String[]::new);
    }

    // ============ 发送（Out）============
    @Override
    public void sendMessage(final Message message)
            throws ConnectionException, SendMessageFailedException, InterruptedException {
        if (state != State.CONNECTED) {
            throw new ConnectionFailedException("连接未就绪");
        }
        try {
            // ============ 动态解析请求字段（从消息属性 / 消息体取值）============
            // 支持 Rhapsody 两种引用写法：${propName} / $propName 以及 ${body} / $body。
            // 例如 URL 配成 "https://api.example.com/${Path}" 即按消息属性 Path 动态拼接；
            // 配成具体地址（不含 ${} / $）则原样使用（静态 URL 仍可用）。
            final String urlStr = TemplateResolver.resolve(config.getUrl(), message);
            if (urlStr == null || urlStr.trim().isEmpty()) {
                throw new ConnectionFailedException(
                        "解析后的请求URL为空，请检查通讯点 URL 配置或对应消息属性是否存在");
            }
            final String method = TemplateResolver.resolve(config.getMethod(), message)
                    .trim().toUpperCase(Locale.ROOT);
            final String contentType = TemplateResolver.resolve(config.getContentType(), message);
            final String headersStr = TemplateResolver.resolve(config.getHeaders(), message);

            final byte[] body = Messages.asBytes(message);
            final URL url = new URL(urlStr.trim());
            final HttpURLConnection conn = (HttpURLConnection) url.openConnection();
            conn.setInstanceFollowRedirects(true);
            setMethod(conn, method);
            conn.setConnectTimeout(config.getTimeout());
            conn.setReadTimeout(config.getTimeout());

            if (conn instanceof HttpsURLConnection) {
                final HttpsURLConnection https = (HttpsURLConnection) conn;
                https.setSSLSocketFactory(sslSocketFactory);
                https.setHostnameVerifier(hostnameVerifier);
            }

            conn.setRequestProperty("User-Agent", USER_AGENT);
            if (contentType != null && !contentType.trim().isEmpty()) {
                conn.setRequestProperty("Content-Type", contentType.trim());
            }
            applyCustomHeaders(conn, headersStr);

            final boolean hasBody = body != null && body.length > 0
                    && !"GET".equals(method) && !"HEAD".equals(method) && !"DELETE".equals(method);
            if (hasBody) {
                conn.setDoOutput(true);
                try (OutputStream os = conn.getOutputStream()) {
                    os.write(body);
                    os.flush();
                }
            }

            final int code = conn.getResponseCode();
            final String statusText = conn.getResponseMessage();
            // 4xx/5xx 时 getInputStream 会抛异常，改用 getErrorStream；401/403 等仍可能有响应体在输入流
            InputStream in;
            try {
                in = conn.getInputStream();
            } catch (final IOException e) {
                in = conn.getErrorStream();
            }
            final byte[] respBody = (in == null) ? new byte[0] : readAll(in);
            final String respContentType = conn.getContentType();
            final Map<String, String> respHeaders = extractHeaders(conn);
            conn.disconnect();

            this.pendingResponse = new PendingResponse(code, statusText, respContentType, respHeaders, respBody);
            log(Level.DEBUG, "HttpRestful 请求完成 method=" + method + " url=" + urlStr
                    + " status=" + code);
        } catch (final IOException e) {
            state = State.ERROR;
            throw new ConnectionFailedException("HTTP请求失败:" + e.getMessage(), e);
        }
    }

    private static void setMethod(final HttpURLConnection conn, final String method)
            throws ConnectionException {
        final String m = (method == null) ? "POST" : method.trim().toUpperCase(Locale.ROOT);
        try {
            conn.setRequestMethod(m);
        } catch (final ProtocolException pe) {
            // 非标准方法（如 PATCH）JDK 不允许直接 set，改用反射设置私有 method 字段
            try {
                final java.lang.reflect.Field f = conn.getClass().getDeclaredField("method");
                f.setAccessible(true);
                f.set(conn, m);
            } catch (final Exception ex) {
                throw new ConnectionFailedException("不支持的HTTP方法:" + m, ex);
            }
        }
    }

    private static void applyCustomHeaders(final HttpURLConnection conn, final String headers) {
        if (headers == null || headers.trim().isEmpty()) {
            return;
        }
        for (final String line : headers.split("\\r?\\n")) {
            final String t = line.trim();
            if (t.isEmpty()) {
                continue;
            }
            final int idx = t.indexOf(':');
            if (idx <= 0) {
                continue;
            }
            conn.setRequestProperty(t.substring(0, idx).trim(), t.substring(idx + 1).trim());
        }
    }

    private static Map<String, String> extractHeaders(final HttpURLConnection conn) {
        final Map<String, String> map = new LinkedHashMap<>();
        for (final Map.Entry<String, List<String>> e : conn.getHeaderFields().entrySet()) {
            final String k = e.getKey();
            if (k == null) {
                continue;
            }
            final List<String> v = e.getValue();
            map.put(k, (v != null && !v.isEmpty()) ? v.get(0) : "");
        }
        return map;
    }

    private static byte[] readAll(final InputStream in) throws IOException {
        final ByteArrayOutputStream bos = new ByteArrayOutputStream();
        final byte[] buf = new byte[8192];
        int n;
        while ((n = in.read(buf)) != -1) {
            bos.write(buf, 0, n);
        }
        return bos.toByteArray();
    }

    // ============ 接收（In）============
    @Override
    public boolean receiveMessage(final Message message)
            throws ConnectionException, MessageException, InterruptedException {
        final PendingResponse r = this.pendingResponse;
        if (r == null) {
            return false;
        }
        try {
            Messages.setBody(message, (r.body == null) ? new byte[0] : r.body);
        } catch (final IOException e) {
            throw new MessageException("写入HTTP响应到Rhapsody消息体失败", e);
        }

        final String statusProp = (config.getPropStatus() != null && !config.getPropStatus().trim().isEmpty())
                ? config.getPropStatus().trim() : "http_status";
        message.getProperty(statusProp).setValue(String.valueOf(r.statusCode));
        message.getProperty("http_statusText").setValue(r.statusText == null ? "" : r.statusText);
        if (r.contentType != null) {
            message.getProperty("http_contentType").setValue(r.contentType);
        }

        final String prefix = config.getRespHeaderPrefix();
        if (prefix != null && !prefix.trim().isEmpty()) {
            for (final Map.Entry<String, String> e : r.headers.entrySet()) {
                message.getProperty(prefix.trim() + sanitize(e.getKey())).setValue(e.getValue());
            }
        }

        this.pendingResponse = null;
        return true;
    }

    private static String sanitize(final String name) {
        final StringBuilder sb = new StringBuilder();
        for (int i = 0; i < name.length(); i++) {
            final char c = name.charAt(i);
            sb.append(Character.isLetterOrDigit(c) ? c : '_');
        }
        return sb.toString();
    }

    @Override
    public void messageProcessed(final boolean success) throws ConnectionException {
        this.pendingResponse = null;
    }

    @Override
    public void initSend() throws ConnectionException, InterruptedException {
        if (state == State.ERROR) {
            throw new ConnectionFailedException("连接状态异常");
        }
    }

    @Override
    public void finishSend() throws InterruptedException {
        // 每条消息独立 HTTP 请求，无需额外刷新
    }

    // ============ 生命周期 ============
    @Override
    public int getId() {
        return id;
    }

    @Override
    public State getState() {
        return state;
    }

    @Override
    public void dropConnection() {
        this.pendingResponse = null;
        this.state = State.UNCONNECTED;
    }

    @Override
    public void reconnect() throws ConnectionException, InterruptedException {
        this.pendingResponse = null;
        prepareSsl();
        this.state = State.CONNECTED;
    }

    @Override
    public void finishComms() {
    }

    @Override
    public void cleanup() {
        this.pendingResponse = null;
    }

    private void log(final Level level, final String msg) {
        if (logger != null) {
            logger.logLiteral(level, msg);
        }
    }
}
