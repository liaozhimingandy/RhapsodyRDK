/**
 * 自定义通讯点（CommunicationPoint）实现包。
 *
 * <p>新增一个通讯点的完整步骤：
 * <ol>
 *   <li>在本包下创建通讯点类，继承框架的通讯点基类，实现收发逻辑；</li>
 *   <li>实现 {@code getPropertyList()} 声明配置项。每条配置串必须严格 6 段：
 *       {@code Name|Type|DefaultValue|Display Name|Dependency|Description}。
 *       段数不足编译期毫无提示，但引擎注册通讯点类型时会抛
 *       {@code ArrayIndexOutOfBoundsException}，现象是「引擎能启动、管理控制台登录不上」。
 *       提交前先用 rhapsody-component-dev 技能的 check_props.py 自检；</li>
 *   <li>在 {@code com.alsoapp.rhapsody.rdk.Activator#activate} 中用
 *       {@code CommunicationPointRegistration} 登记本类，框架才会在路由设计器里列出它。</li>
 * </ol>
 *
 * <p>依赖第三方库（如 kafka-clients）时，除了放进 {@code lib/} 并写进
 * {@code Bundle-ClassPath}，还要在 {@code META-INF/MANIFEST.MF} 的
 * {@code Import-Package} 里补上用到的框架包，否则部署时会报
 * Unresolved requirement，导致整个 bundle 起不来。
 */
package com.alsoapp.rhapsody.rdk.commpoint;
