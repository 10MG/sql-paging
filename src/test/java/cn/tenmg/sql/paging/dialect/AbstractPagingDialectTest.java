package cn.tenmg.sql.paging.dialect;

import java.util.Iterator;
import java.util.Map.Entry;
import java.util.Properties;

import cn.tenmg.dsl.utils.PlaceHolderUtils;
import cn.tenmg.dsl.utils.PropertiesLoaderUtils;

public abstract class AbstractPagingDialectTest {

	/**
	 * 加载配置
	 * 
	 * @param path 配置文件位置（相对于classpath）
	 * @return 配置
	 */
	protected static Properties loadConfig(String path) {
		Properties config = new Properties();
		config.putAll(System.getProperties());
		PropertiesLoaderUtils.loadIgnoreException(config, path);
		for (Iterator<Entry<Object, Object>> it = config.entrySet().iterator(); it.hasNext();) {
			Entry<Object, Object> entry = it.next();
			Object value = entry.getValue();
			if (value instanceof String) {
				config.put(entry.getKey(), PlaceHolderUtils.replace((String) value, config));
			}
		}
		return config;
	}

}
