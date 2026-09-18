package com.alsoapp.rhapsody.rdk.filter.redis;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.orchestral.rhapsody.message.Message;
import com.orchestral.rhapsody.message.Messages;

/**
 * 把配置里的模板字符串解析成实际值，兼容 Rhapsody 原生的两种属性引用写法：
 *   - 花括号形式 ：${propName} / ${body}
 *   - 简写形式   ：$propName / $body
 * 例：KEY = "msg:${MessageId}" 或 "msg:$MessageId"，VALUE = "${body}" 或 "$body"。
 * 实现与 Rhapsody 动态配置保持一致，用户两种写法都能用。
 */
public final class TemplateResolver {

    // 先匹配 ${...} 花括号形式，再匹配 $name 简写形式（name 为合法标识符）。
    private static final Pattern TOKEN =
            Pattern.compile("\\$\\{([^}]+)\\}|\\$([A-Za-z_][A-Za-z0-9_]*)");

    private TemplateResolver() {}

    public static String resolve(String template, Message msg) {
        if (template == null || template.isEmpty()) {
            return template;
        }
        Matcher m = TOKEN.matcher(template);
        StringBuffer sb = new StringBuffer();
        while (m.find()) {
            // 花括号形式命中 group(1)，简写形式命中 group(2)，二者必有其一。
            String token = m.group(1) != null ? m.group(1) : m.group(2);
            if (token == null) {
                m.appendReplacement(sb, Matcher.quoteReplacement(m.group(0)));
                continue;
            }
            token = token.trim();
            String value;
            if ("body".equalsIgnoreCase(token)) {
                try {
                    value = Messages.asString(msg);
                } catch (Exception e) {
                    value = "";
                }
            } else {
                value = readMessageProperty(msg, token);
            }
            m.appendReplacement(sb, Matcher.quoteReplacement(value == null ? "" : value));
        }
        m.appendTail(sb);
        return sb.toString();
    }

    private static String readMessageProperty(Message msg, String name) {
        try {
            return msg.getProperty(name).getValue();
        } catch (Exception e) {
            return null;
        }
    }
}
