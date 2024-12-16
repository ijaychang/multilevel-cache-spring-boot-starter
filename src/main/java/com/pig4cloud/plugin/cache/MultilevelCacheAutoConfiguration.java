package com.pig4cloud.plugin.cache;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.PropertyAccessor;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.*;
import com.fasterxml.jackson.databind.deser.std.StdDeserializer;
import com.fasterxml.jackson.databind.jsontype.impl.LaissezFaireSubTypeValidator;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.fasterxml.jackson.databind.ser.std.StdSerializer;
import com.pig4cloud.plugin.cache.properties.CacheConfigProperties;
import com.pig4cloud.plugin.cache.support.CacheMessageListener;
import com.pig4cloud.plugin.cache.support.RedisCaffeineCacheManager;
import com.pig4cloud.plugin.cache.support.RedisCaffeineCacheManagerCustomizer;
import com.pig4cloud.plugin.cache.support.ServerIdGenerator;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.autoconfigure.AutoConfigureAfter;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.data.redis.RedisAutoConfiguration;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.cache.support.NullValue;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.redis.connection.RedisConnectionFactory;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.listener.ChannelTopic;
import org.springframework.data.redis.listener.RedisMessageListenerContainer;
import org.springframework.data.redis.serializer.Jackson2JsonRedisSerializer;
import org.springframework.data.redis.serializer.RedisSerializer;
import org.springframework.data.redis.serializer.StringRedisSerializer;

import java.io.IOException;
import java.util.Objects;

/**
 * @author fuwei.deng
 * @version 1.0.0
 */
@Configuration(proxyBeanMethods = false)
@AutoConfigureAfter(RedisAutoConfiguration.class)
@EnableConfigurationProperties(CacheConfigProperties.class)
public class MultilevelCacheAutoConfiguration {

	@Bean
	@ConditionalOnMissingBean
	@ConditionalOnBean(RedisTemplate.class)
	public RedisCaffeineCacheManager cacheManager(CacheConfigProperties cacheConfigProperties,
			@Qualifier("stringKeyRedisTemplate") RedisTemplate<Object, Object> stringKeyRedisTemplate,
			ObjectProvider<RedisCaffeineCacheManagerCustomizer> cacheManagerCustomizers,
			ObjectProvider<ServerIdGenerator> serverIdGenerators) {
		Object serverId = cacheConfigProperties.getServerId();
		if (serverId == null || "".equals(serverId)) {
			serverIdGenerators
					.ifAvailable(serverIdGenerator -> cacheConfigProperties.setServerId(serverIdGenerator.get()));
		}
		RedisCaffeineCacheManager cacheManager = new RedisCaffeineCacheManager(cacheConfigProperties,
				stringKeyRedisTemplate);
		cacheManagerCustomizers.orderedStream().forEach(customizer -> customizer.customize(cacheManager));
		return cacheManager;
	}

	/**
	 * 可自定义名称为stringKeyRedisTemplate的RedisTemplate覆盖掉默认RedisTemplate。
	 */
	@Bean
	@ConditionalOnMissingBean(name = "stringKeyRedisTemplate")
	public RedisTemplate<Object, Object> stringKeyRedisTemplate(RedisConnectionFactory redisConnectionFactory) {
		RedisTemplate<Object, Object> redisTemplate = new RedisTemplate<>();
		redisTemplate.setConnectionFactory(redisConnectionFactory);

		// 用Jackson2JsonRedisSerializer来序列化和反序列化redis的value值
		Jackson2JsonRedisSerializer<Object> jackson2JsonRedisSerializer = new Jackson2JsonRedisSerializer<>(Object.class);
		ObjectMapper objectMapper = new ObjectMapper();
		// 指定要序列化的域(field,get,set)，访问修饰符(public,private,protected)
		objectMapper.setVisibility(PropertyAccessor.ALL, JsonAutoDetect.Visibility.ANY);
		// 修复com.fasterxml.jackson.databind.exc.InvalidDefinitionException: No serializer found for class org.springframework.cache.support.NullValue and no properties discovered to create BeanSerializer (to avoid exception, disable SerializationFeature.FAIL_ON_EMPTY_BEANS) 报错
		objectMapper.configure(SerializationFeature.FAIL_ON_EMPTY_BEANS, false);
		objectMapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);

		// Validator验证类用于验证是否能够被反序列化,DefaultTyping指定序列化输入的类型，类必须是非final修饰的，final修饰的类，比如String,Integer等会跑出异常
		objectMapper.activateDefaultTyping(LaissezFaireSubTypeValidator.instance, ObjectMapper.DefaultTyping.NON_FINAL);

		// 注册 org.springframework.cache.support.NullValue 自定义的序列化，反序列化处理
		SimpleModule nullValueSimpleModule = new SimpleModule();
		nullValueSimpleModule.addSerializer(NullValue.class, NullValueSerializer.INSTANCE);
		nullValueSimpleModule.addDeserializer(NullValue.class, NullValueDeserializer.INSTANCE);
		objectMapper.registerModule(nullValueSimpleModule);

		jackson2JsonRedisSerializer.setObjectMapper(objectMapper);

		StringRedisSerializer stringRedisSerializer = new StringRedisSerializer();

		// key采用String的序列化方式
		redisTemplate.setKeySerializer(stringRedisSerializer);
		// hash的key也采用String的序列化方式
		redisTemplate.setHashKeySerializer(stringRedisSerializer);
		// value序列化方式采用jackson
		redisTemplate.setValueSerializer(jackson2JsonRedisSerializer);
		// hash的value序列化方式采用jackson
		redisTemplate.setHashValueSerializer(jackson2JsonRedisSerializer);

		redisTemplate.afterPropertiesSet();
		return redisTemplate;
	}

	@Bean
	@ConditionalOnMissingBean(name = "cacheMessageListenerContainer")
	public RedisMessageListenerContainer cacheMessageListenerContainer(CacheConfigProperties cacheConfigProperties,
			@Qualifier("stringKeyRedisTemplate") RedisTemplate<Object, Object> stringKeyRedisTemplate,
			@Qualifier("cacheMessageListener") CacheMessageListener cacheMessageListener) {
		RedisMessageListenerContainer redisMessageListenerContainer = new RedisMessageListenerContainer();
		redisMessageListenerContainer
				.setConnectionFactory(Objects.requireNonNull(stringKeyRedisTemplate.getConnectionFactory()));
		redisMessageListenerContainer.addMessageListener(cacheMessageListener,
				new ChannelTopic(cacheConfigProperties.getRedis().getTopic()));
		return redisMessageListenerContainer;
	}

	@Bean
	@SuppressWarnings("unchecked")
	@ConditionalOnMissingBean(name = "cacheMessageListener")
	public CacheMessageListener cacheMessageListener(
			@Qualifier("stringKeyRedisTemplate") RedisTemplate<Object, Object> stringKeyRedisTemplate,
			RedisCaffeineCacheManager redisCaffeineCacheManager) {
		return new CacheMessageListener((RedisSerializer<Object>) stringKeyRedisTemplate.getValueSerializer(),
				redisCaffeineCacheManager);
	}

	public static class NullValueDeserializer extends StdDeserializer<NullValue> {

		public static final NullValueDeserializer INSTANCE = new NullValueDeserializer();

		protected NullValueDeserializer() {
			super(NullValue.class);
		}

		@Override
		public NullValue deserialize(JsonParser p, DeserializationContext ctx) {
			return (NullValue) NullValue.INSTANCE;
		}

	}

	public static class NullValueSerializer extends StdSerializer<NullValue> {

		public static final NullValueSerializer INSTANCE = new NullValueSerializer();

		protected NullValueSerializer() {
			super(NullValue.class);
		}


		@Override
		public void serialize(NullValue value, JsonGenerator gen, SerializerProvider provider) throws IOException {
			gen.writeNull();
		}
	}

}
