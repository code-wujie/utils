package com.utils.dubbo.consumer;

import com.alibaba.dubbo.config.ApplicationConfig;
import com.alibaba.dubbo.config.ConsumerConfig;
import com.alibaba.dubbo.config.ReferenceConfig;
import com.alibaba.dubbo.config.RegistryConfig;
import com.utils.dubbo.consumer.model.Contexts;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

/**
 * Created by WJ on 2018/6/13.
 */
public class DubboConsumer<T> {
    private static final Logger logger = LogManager.getLogger(DubboConsumer.class);
    private Class<?> IClass;
    private ApplicationConfig applicationConfig = new ApplicationConfig();
    private RegistryConfig registryConfig = new RegistryConfig();
    private ConsumerConfig consumerConfig = new ConsumerConfig();

    /**
     * 构造函数，必须传入要消费的接口类
     * @param interfaceClass
     */
    public DubboConsumer(Class<?> interfaceClass) {
        this.IClass = interfaceClass;
        this.loadDefaultConfig();
    }

    /**
     * 获取dubbo的服务。
     * @return 返回传入类型的接口的消费对象，使用时候需要强转为接口类
     */
    public T getService() {
        ReferenceConfig<T> referenceConfig = new ReferenceConfig();
        referenceConfig.setApplication(this.applicationConfig);
        referenceConfig.setRegistry(this.registryConfig);
        referenceConfig.setConsumer(this.consumerConfig);
        referenceConfig.setInterface(this.IClass);
        referenceConfig.setVersion("1.0.0");
        return referenceConfig.get();
    }

    /**
     * 加载参数，若需要修改参数，到contexts的类中进行修改。后续可以和Apollo配置中心结合起来使用。
     * 其他参数使用默认。
     */
    private void loadDefaultConfig() {
        String appName = Contexts.DUBBO_CONSUMER_APPLICATION_NAME;
        logger.info("Dubbo消费者应用名：" + appName);
        this.applicationConfig.setName(appName);
        String version=Contexts.DUBBO_APPLICATION_VERSION;
        logger.info("Dubbo消费版本为：" + version);
        this.applicationConfig.setVersion(version);
        String protocol = Contexts.DUBBO_REGISTER_PROTOCOL;
        logger.info("Dubbo协议：" + protocol);
        this.registryConfig.setProtocol(protocol);
        String regAddress = Contexts.DUBBO_REGISTER_ADDRESS;
        logger.info("Dubbo注册中心地址：" + regAddress);
        this.registryConfig.setAddress(regAddress);
        Integer consumerTimeout = Contexts.DUBBO_COMSUMER_TIMEOUT;
        logger.info("Dubbo消费者超时时间：" + consumerTimeout + "ms");
        this.consumerConfig.setTimeout(consumerTimeout);
        Integer retries = Contexts.DUBBO_COMSUMER_RETRIES;
        logger.info("Dubbo消费者重试次数：" + retries);
        this.consumerConfig.setRetries(retries);
    }

    public ApplicationConfig getApplicationConfig() {
        return this.applicationConfig;
    }

    public RegistryConfig getRegistryConfig() {
        return this.registryConfig;
    }

    public ConsumerConfig getConsumerConfig() {
        return this.consumerConfig;
    }
}
