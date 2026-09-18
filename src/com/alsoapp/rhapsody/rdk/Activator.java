package com.alsoapp.rhapsody.rdk;

import java.io.FileNotFoundException;
import java.util.HashSet;
import java.util.Set;

import javax.xml.parsers.DocumentBuilderFactory;

import org.osgi.framework.BundleContext;
import org.osgi.framework.ServiceRegistration;
import org.osgi.service.component.ComponentContext;

import com.orchestral.rhapsody.configuration.auxiliaryfiles.AuxiliaryFilesService;
import com.orchestral.rhapsody.configuration.security.SecurityObjectsService;
import com.orchestral.rhapsody.configuration.variables.VariablesService;
import com.orchestral.rhapsody.idgenerator.IdGeneratorFactory;
import com.orchestral.rhapsody.messageparsing.MessageParsingService;
import com.orchestral.rhapsody.model.definition.v_1_1.DefinitionService;
import com.orchestral.rhapsody.module.CommunicationPointRegistration;
import com.orchestral.rhapsody.module.FilterRegistration;
import com.orchestral.rhapsody.module.communicationpoint.CommunicationPointInfo;
import com.orchestral.rhapsody.persistentmap.PersistentMap;
import com.orchestral.rhapsody.persistentmap.PersistentMapService;
import com.orchestral.rhapsody.security.SecurityException;

import com.alsoapp.rhapsody.rdk.commpoint.kafka.KafkaCommPoint;
import com.alsoapp.rhapsody.rdk.filter.redis.RedisClusterFilter;
import com.alsoapp.rhapsody.rdk.filter.redis.RedisSentinelFilter;

/**
 * 本 Bundle 的 DS 组件入口。
 *
 * <p>对应声明在 {@code OSGI-INF/Service.xml}，由 MANIFEST.MF 的
 * {@code Service-Component} 头引用。框架（Rhapsody 引擎）激活本组件时，
 * 会先把下面 8 个服务注入进来，再调用 {@link #activate(ComponentContext)}。
 *
 * <p>新增通讯点 / 过滤器时，在 activate() 里登记即可，例如：
 *
 * <pre>
 * String cpr = com.orchestral.rhapsody.module.CommunicationPointRegistration.class.getName();
 * String fr  = com.orchestral.rhapsody.module.FilterRegistration.class.getName();
 *
 * registrations.add(context.registerService(cpr,
 *         new com.orchestral.rhapsody.module.CommunicationPointRegistration(
 *                 "com.alsoapp.rhapsody.rdk.commpoint.MyCommPoint", "我的通讯点",
 *                 com.alsoapp.rhapsody.rdk.commpoint.MyCommPoint.class, null, null, null), null));
 *
 * registrations.add(context.registerService(fr,
 *         new com.orchestral.rhapsody.module.FilterRegistration(
 *                 "com.alsoapp.rhapsody.rdk.filter.MyFilter", "我的过滤器", "RDK",
 *                 com.alsoapp.rhapsody.rdk.filter.MyFilter.class, null, null), null));
 * </pre>
 */
public class Activator {

	private static AuxiliaryFilesService auxiliaryFilesService;
	private static DefinitionService definitionService;
	private static MessageParsingService messageParsingService;
	private static PersistentMapService persistentMapService;
	private static SecurityObjectsService securityObjectsService;
	private static DocumentBuilderFactory documentBuilderFactory;
	private static IdGeneratorFactory idGeneratorFactory;
	private static VariablesService variablesService;

	private final Set<ServiceRegistration<?>> registrations = new HashSet<ServiceRegistration<?>>();

	protected void setAuxiliaryFilesService(final AuxiliaryFilesService auxiliaryFilesService) {
		Activator.auxiliaryFilesService = auxiliaryFilesService;
	}

	protected void unsetAuxiliaryFilesService(final AuxiliaryFilesService auxiliaryFilesService) {
		Activator.auxiliaryFilesService = null;
	}

	protected void setDefinitionService(final DefinitionService definitionService) {
		Activator.definitionService = definitionService;
	}

	protected void unsetDefinitionService(final DefinitionService definitionService) {
		Activator.definitionService = null;
	}

	protected void setMessageParsingService(final MessageParsingService messageParsingService) {
		Activator.messageParsingService = messageParsingService;
	}

	protected void unsetMessageParsingService(final MessageParsingService messageParsingService) {
		Activator.messageParsingService = null;
	}

	protected void setPersistentMapService(final PersistentMapService persistentMapService) {
		Activator.persistentMapService = persistentMapService;
	}

	protected void unsetPersistentMapService(final PersistentMapService persistentMapService) {
		Activator.persistentMapService = null;
	}

	protected void setSecurityObjectsService(final SecurityObjectsService securityObjectsService) {
		Activator.securityObjectsService = securityObjectsService;
	}

	protected void unsetSecurityObjectsService(final SecurityObjectsService securityObjectsService) {
		Activator.securityObjectsService = null;
	}

	public void setDocumentBuilderFactory(final DocumentBuilderFactory factory) {
		Activator.documentBuilderFactory = factory;
	}

	protected void unsetDocumentBuilderFactory(final DocumentBuilderFactory factory) {
		Activator.documentBuilderFactory = null;
	}

	protected void setIdGeneratorFactory(final IdGeneratorFactory idGeneratorFactory) {
		Activator.idGeneratorFactory = idGeneratorFactory;
	}

	protected void unsetIdGeneratorFactory(final IdGeneratorFactory idGeneratorFactory) {
		Activator.idGeneratorFactory = null;
	}

	protected void setVariablesService(final VariablesService variablesService) {
		Activator.variablesService = variablesService;
	}

	protected void unsetVariablesService(final VariablesService variablesService) {
		Activator.variablesService = null;
	}

	public static AuxiliaryFilesService getAuxiliaryFilesService() throws FileNotFoundException, SecurityException {
		return Activator.auxiliaryFilesService;
	}

	public static DefinitionService getDefinitionService() {
		return Activator.definitionService;
	}

	public static MessageParsingService getMessageParsingService() {
		return Activator.messageParsingService;
	}

	public static PersistentMap getPersistentMap(final CommunicationPointInfo communicationPointInfo) {
		return Activator.persistentMapService.getPersistentMap(communicationPointInfo);
	}

	public static SecurityObjectsService getSecurityObjectsService() {
		return Activator.securityObjectsService;
	}

	public static DocumentBuilderFactory getDocumentBuilderFactory() {
		return Activator.documentBuilderFactory;
	}

	public static IdGeneratorFactory getIdGeneratorFactory() {
		return Activator.idGeneratorFactory;
	}

	public static VariablesService getVariablesService() {
		return Activator.variablesService;
	}

	protected void activate(final ComponentContext componentContext) {
		// 通讯点 / 过滤器在这里登记，示例见类注释。
		// 登记的对象会在 deactivate() 中统一注销。
		
		final BundleContext context = componentContext.getBundleContext();
		final String cpr = CommunicationPointRegistration.class.getName();
		final String fr = FilterRegistration.class.getName();

		// Register the communication point types in this module
		// this.registrations.add(context.registerService(cpr, new
		// CommunicationPointRegistration("Directory", "Directory",
		// DirectoryFileCommunicationPoint.class, "/file.bmp", null,
		// DirectoryFileCommunicationPoint.RECORDS), null));

		// Register the filter types in this module
		// this.registrations.add(context.registerService(fr, new
		// FilterRegistration("AsymmetricCrypto", "Asymmetric Cryptography",
		// "Utility", AsymmetricCryptoFilter.class, "/crypto.bmp", null),
		// null));

		//this.registrations.add(context.registerService(fr, new FilterRegistration("com.alsoapp.rhapsody.module.test.examples.MessageParsingFilter", "Message Parsing Filter", "RDK", MessageParsingFilter.class, null, null), null)); 
 		//this.registrations.add(context.registerService(fr, new FilterRegistration("com.alsoapp.rhapsody.module.test.examples.ReplaceBody", "Replace Body", "RDK", ReplaceBody.class, null, null), null)); 
		//this.registrations.add(context.registerService(fr, new FilterRegistration("com.alsoapp.rhapsody.module.test.UpperCaseFilter", "UpperCaseFilter", "测试", UpperCaseFilter.class, null, null), null));		
		//this.registrations.add(context.registerService(fr, new FilterRegistration("com.alsoapp.rhapsody.module.test.AppendFilter", "AppendFilter", "测试", AppendFilter.class, null, null), null));		
		
		// 注册通讯点
		this.registrations.add(context.registerService(cpr, new CommunicationPointRegistration("com.alsoapp.rhapsody.rdk.commpoint.kafka", 
				"openKafka", KafkaCommPoint.class, "/icons/kafka32.bmp", null, null), null));
		
		// 注册过滤器
		// Redis 过滤器：Cluster 经 Predixy 代理单端点，Sentinel 走哨兵发现 master
		this.registrations.add(context.registerService(fr, new FilterRegistration(
				"com.alsoapp.rhapsody.rdk.filter.redis.RedisClusterFilter", "Redis Cluster", "Redis",
				RedisClusterFilter.class, 
				"/icons/redis32.bmp",  // 32x32 图标（路由中显示）
				"/icons/redis16.bmp"	// 16x16 图标（工具箱显示）
				), null));
		this.registrations.add(context.registerService(fr, new FilterRegistration(
				"com.alsoapp.rhapsody.rdk.filter.redis.RedisSentinelFilter", "Redis Sentinel", "Redis",
				RedisSentinelFilter.class, 
				"/icons/redis32.bmp",  // 32x32 图标（路由中显示）
				"/icons/redis16.bmp"	// 16x16 图标（工具箱显示）
				), null));
		/* <START> - CODE CONTROLLED BY WIZARDS DO NOT REMOVE COMMENT */
		/* <END> - CODE CONTROLLED BY WIZARDS DO NOT REMOVE COMMENT */
		/* <START> - CODE CONTROLLED BY WIZARDS DO NOT REMOVE COMMENT */
		/* <END> - CODE CONTROLLED BY WIZARDS DO NOT REMOVE COMMENT */
	}

	protected void deactivate(final ComponentContext context) {
		for (final ServiceRegistration<?> registration : this.registrations) {
			registration.unregister();
		}
		this.registrations.clear();
	}

}
