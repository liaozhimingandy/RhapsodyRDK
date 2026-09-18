package com.alsoapp.rhapsody.rdk.commpoint.httprestful;

import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLSocket;
import javax.net.ssl.SSLSocketFactory;
import java.io.IOException;
import java.net.InetAddress;
import java.net.Socket;
import java.net.UnknownHostException;

/**
 * 包裹 JDK 默认 SSLSocketFactory，对每一个新建的 SSLSocket 做两件事：
 * <ol>
 *   <li>放开加密套件：把 {@code setEnabledCipherSuites} 设为
 *       {@code getSupportedCipherSuites()}（全部受支持套件），
 *       这正是解决「内置 HTTP 客户端加密套件太少、HTTPS 握手失败」的关键。</li>
 *   <li>按需限定 TLS 协议版本（如 TLSv1.2 / TLSv1.3）。</li>
 * </ol>
 *
 * <p>注意：默认启用的套件列表是 JDK 人为收窄过的；把 enabled 设为 supported
 * 后，握手时客户端将向服务端提供完整套件列表，由双方协商，
 * 与 Postman / 现代浏览器的行为一致。
 */
public class HttpRestfulSSLSocketFactory extends SSLSocketFactory {

    private final SSLSocketFactory delegate;
    private final String[] enabledProtocols;
    private final boolean enableAllCiphers;

    public HttpRestfulSSLSocketFactory(final SSLContext context, final String[] enabledProtocols,
                                   final boolean enableAllCiphers) {
        this.delegate = context.getSocketFactory();
        this.enabledProtocols = enabledProtocols;
        this.enableAllCiphers = enableAllCiphers;
    }

    private Socket configure(final Socket socket) {
        if (socket instanceof SSLSocket) {
            final SSLSocket ssl = (SSLSocket) socket;
            if (enabledProtocols != null && enabledProtocols.length > 0) {
                try {
                    ssl.setEnabledProtocols(enabledProtocols);
                } catch (final IllegalArgumentException ignored) {
                    // 配置了引擎/服务端不支持的协议时忽略，回退到默认
                }
            }
            if (enableAllCiphers) {
                ssl.setEnabledCipherSuites(ssl.getSupportedCipherSuites());
            }
        }
        return socket;
    }

    @Override
    public String[] getDefaultCipherSuites() {
        return delegate.getDefaultCipherSuites();
    }

    @Override
    public String[] getSupportedCipherSuites() {
        return delegate.getSupportedCipherSuites();
    }

    @Override
    public Socket createSocket() throws IOException {
        return configure(delegate.createSocket());
    }

    @Override
    public Socket createSocket(final Socket s, final String host, final int port, final boolean autoClose)
            throws IOException {
        return configure(delegate.createSocket(s, host, port, autoClose));
    }

    @Override
    public Socket createSocket(final String host, final int port) throws IOException, UnknownHostException {
        return configure(delegate.createSocket(host, port));
    }

    @Override
    public Socket createSocket(final String host, final int port, final InetAddress localHost, final int localPort)
            throws IOException, UnknownHostException {
        return configure(delegate.createSocket(host, port, localHost, localPort));
    }

    @Override
    public Socket createSocket(final InetAddress host, final int port) throws IOException {
        return configure(delegate.createSocket(host, port));
    }

    @Override
    public Socket createSocket(final InetAddress host, final int port, final InetAddress localHost, final int localPort)
            throws IOException {
        return configure(delegate.createSocket(host, port, localHost, localPort));
    }
}
