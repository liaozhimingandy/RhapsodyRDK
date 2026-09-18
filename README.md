# RhapsodyRDK
Rhapsody组件二次开发

### Kafka需要下载对应jar包放到lib下

##### kafka用到以下jar包
```
kafka-clients-4.3.1.jar
slf4j-api-1.7.36.jar
snappy-java-1.1.10.7.jar
lz4-java-1.10.2.jar
```

##### redis用到以下jar包
```
commons-pool2-2.11.1.jar
jedis-4.4.6.jar
slf4j-api-1.7.36.jar
```

### 注意的点
```
1.过滤器和通讯点必须实现getPropertyList();可以返回空字符串new String[0],否则引擎无法识别
2.MANIFEST.MF文件添加类 -> Runtime -> Add 增加依赖的lib和icons下的内容

```


## 2. 构建方式

### A. Eclipse RDK（推荐，与官方手册一致）

#### 0) 前置准备（只需一次）
1. **JDK 11**：Rhapsody 7 对应 JDK 11（与 Rhapsody 引擎版本严格一致）。
2. **Eclipse**：必须选 **"Eclipse IDE for RCP and RAP Developers"** 版本，其他版本无法解析 RDK Wizard。
3. **RDK Wizard**：获取 `com.orchestral.rhapsody.eclipsewizard.jar`（版本必须与 Rhapsody 引擎版本一致，如 Rhapsody 7 配 RDK7），放到 Eclipse 安装目录的 `plugins` 文件夹下。以管理员身份打开 Eclipse，若 `File → New → Other` 中能看到 **Rhapsody Wizards** 即安装成功。
4. **Rhapsody 引擎已安装**：本机需有 Rhapsody 引擎目录（含 `plugins/` 与 `data/modules/`），以便配置 Target Platform 与部署。

#### 1) 配置 Eclipse（只需一次，对应手册 4.x）
- **1.1 新增 Rhapsody JRE**：`Window → Preferences → Java → Installed JREs → Add → Standard VM`，JRE 目录指向 Rhapsody 引擎安装目录下的 `jre` 文件夹，勾选设为默认（JRE 名建议取 "Rhapsody 7"）。
- **1.2 配置 Target Platform**：`Window → Preferences → Plug-in Development → Target Platform → Add → 空定义开始 → Locations → Add → Directory`，浏览到 Rhapsody 引擎的 `plugins` 文件夹 → 命名（如 `Rhapsody 7`）→ Finish → 勾选激活 → Apply。这一步让 `com.orchestral.rhapsody.*` 在编译时可解析。
- **1.3（可选）开发期日志**：编辑 Rhapsody 安装目录下 `rhapsody/logback.xml`，启用 ConsoleAppender，便于在 Eclipse 控制台看日志。

#### 2) 导入工程
- **推荐**：`File → Import → General → Existing Projects into Workspace`，Browse 到本 `rhapsody-redis-connectors` 目录，Eclipse 会识别到工程，勾选后 Finish。
- 已提供 `.project` / `.classpath` / `META-INF/MANIFEST.MF`，导入后会自动作为 **Plug-in Project**。
- **或**：`File → New → Other → Plug-in Project`（RDK Wizard 提供），新建一个空 Plug-in 工程（**不要**勾选 "Generate an Activator"），然后把本工程的文件整体复制进去覆盖。
- 导入后确认：
  - `META-INF/MANIFEST.MF` 的 `Bundle-ClassPath` 三个 jar 已在 `lib/` 下（`jedis-4.4.6.jar` / `commons-pool2-2.11.1.jar` / `slf4j-api-1.7.36.jar`）。
  - `Package Explorer` 中无编译错误（若有 `FilterRegistration` / `ComponentActivator` 标红，按你本地 RDK 版本调整 import 包名，逻辑不变）。

#### 3) （可选）在 Eclipse 内运行/调试 Rhapsody
- `Run → Run Configurations → OSGi Framework → New`；在 **Bundles** 选项卡勾选 Target Platform 下全部包，**除** `com.orchestral.rhapsody.modules.dicom`；
- **Arguments** 选项卡 VM 参数填入手册给出的参数（`-Djava.awt.headless=true -Dosgi.noShutdown=true -Declipse.ignoreApp=true … -Xms256m … -Dorg.osgi.framework.os.name=Win32 …`）；
- **Setting** 选项卡选 Rhapsody 7 Target Platform；**Working directory** 设为 Rhapsody 引擎安装目录。
- 运行前**务必停止 Rhapsody 服务**；运行后可用管理控制台查看引擎启动情况。

#### 4) 导出可用 JAR
- 右键工程 → `Export → Plug-in Development → Deployable plug-ins and fragments` → 选择保存目录 → Finish。
- 产物：`com.lyniate.rhapsody.module.redis_1.0.0.jar`（已内嵌 Jedis 等依赖）。

#### 5) 部署到 Rhapsody（关键）
1. 将导出的 jar 复制到 Rhapsody 引擎数据目录的 `modules` 文件夹，例如：
   `C:\Rhapsody\Rhapsody Engine 7\rhapsody\data\modules`
2. **必须重启 Rhapsody 引擎服务**（否则自定义组件不会被加载——这一步最容易被忘）。
3. 重启后在 IDE 工具箱的 **Redis** 文件夹下可见 `Redis Cluster` 与 `Redis Sentinel` 两个过滤器。
4. 拖入路由、双击配置属性（HOST/PORT/PASSWORD/KEY/COMMAND…）、发测试消息验证。
5. 排错：组件不出现时查 `rhapsody/log/log.txt` 搜索 `redis` 关键字；确认 RDK Wizard 版本与引擎一致；确认已重启。

### B. Maven（可选）
```bash
# 若用 Maven CLI，需先把 Rhapsody 引擎 API 安装到本地仓库：
mvn install:install-file -Dfile="<Rhapsody>/rhapsody/plugins/com.orchestral.rhapsody.module_<ver>.jar" \
    -DgroupId=com.orchestral.rhapsody -DartifactId=rhapsody-module-api -Dversion=7.0.0 -Dpackaging=jar
# 同理安装 message-api，然后：
mvn clean package
# 产物：target/rhapsody-redis-connectors-1.0.0.jar
```



