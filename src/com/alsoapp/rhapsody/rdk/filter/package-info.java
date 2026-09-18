/**
 * 自定义过滤器（Filter）实现包。
 *
 * <p>新增一个过滤器的完整步骤：
 * <ol>
 *   <li>在本包下创建过滤器类，实现框架的过滤器接口，在方法里处理消息；</li>
 *   <li>需要可视化配置时实现 {@code getPropertyList()}，格式与通讯点一致，
 *       每条严格 6 段：{@code Name|Type|DefaultValue|Display Name|Dependency|Description}；</li>
 *   <li>在 {@code com.alsoapp.rhapsody.rdk.Activator#activate} 中用
 *       {@code FilterRegistration} 登记本类。</li>
 * </ol>
 */
package com.alsoapp.rhapsody.rdk.filter;
