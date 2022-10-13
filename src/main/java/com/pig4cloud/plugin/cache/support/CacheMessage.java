package com.pig4cloud.plugin.cache.support;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;

/**
 * @author fuwei.deng
 * @version 1.0.0
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class CacheMessage implements Serializable {

	private String cacheName;

	private Object key;

}
