package solr;

import org.apache.commons.collections.CollectionUtils;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.HttpSolrClient;
import org.apache.solr.client.solrj.impl.XMLResponseParser;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.common.*;
import org.apache.commons.beanutils.BeanUtils;
import org.slf4j.LoggerFactory;
import sun.plugin.javascript.ReflectUtil;

import java.beans.PropertyDescriptor;
import java.io.IOException;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.net.MalformedURLException;
import java.util.*;

/**
 * @Name solr.SolrUtils
 * @Description
 * @Author Elwyn
 * @Version 2017/7/19
 * @Copyright 上海云辰信息科技有限公司
 **/
public class SolrUtils {
	private static final org.slf4j.Logger logger = LoggerFactory.getLogger(SolrUtils.class);

	/**
	 * solr 服务器访问地址
	 */
	private static String url = "http://localhost:8080/solr/my_solr";

	private static Integer connectionTimeout = 30000; // socket read timeout

	private static Integer defaltMaxConnectionsPerHost = 100;

	private static Integer maxTotalConnections = 100;

	private static Boolean followRedirects = false; // defaults to false
	private static Boolean allowCompression = true;


	/**
	 * @param map key is filed name value,map value is filed value
	 * @return SolrInputDocument
	 */
	public static SolrInputDocument addFileds(Map<String, Object> map, SolrInputDocument document) {

		if (document == null) {
			document = new SolrInputDocument();
		}
		for (Object o : map.keySet()) {
			String key = o.toString();
			document.setField(key, map.get(key));
		}
		return document;
	}


	/**
	 * 建立solr链接，获取 HttpSolrClient
	 *
	 * @return HttpSolrClient
	 */
	public static HttpSolrClient getClient() {
		HttpSolrClient httpSolrClient = null;
		try {
			httpSolrClient = new HttpSolrClient.Builder(url).build();
			httpSolrClient.setParser(new XMLResponseParser());//设定xml文档解析器
			httpSolrClient.setConnectionTimeout(connectionTimeout);//socket read timeout
			httpSolrClient.setAllowCompression(allowCompression);
			httpSolrClient.setMaxTotalConnections(maxTotalConnections);
			httpSolrClient.setDefaultMaxConnectionsPerHost(defaltMaxConnectionsPerHost);
			httpSolrClient.setFollowRedirects(followRedirects);
		} catch (SolrException e) {
			System.out.println("请检查tomcat服务器或端口是否开启!");
			logger.error(e.getMessage());
			e.printStackTrace();
		}
		return httpSolrClient;
	}

	/**
	 * 将SolrDocument 转换为Bean
	 *
	 * @param record
	 * @param clazz
	 * @return bean
	 */
	public static <T> T solrDocToBean(SolrDocument record,Class<T> clazz) {
		T obj = null;
		try {
			obj = (T) clazz.newInstance();
		} catch (InstantiationException | IllegalAccessException e1) {
			e1.printStackTrace();
		}
		Field[] fields = clazz.getDeclaredFields();
		for (Field field : fields) {
			Object value = record.get(field.getName());
			try {
				BeanUtils.setProperty(obj, field.getName(), value);
			} catch (IllegalAccessException | InvocationTargetException e) {
				e.printStackTrace();
			}
		}
		return obj;
	}

	/**
	 * 懒人方法,根据放入的对象自动构建查询条件,(AND连接)
	 * @param client
	 * @param obj
	 * @param <T>
	 * @return 返回参数类型的对象
	 * @throws IllegalAccessException
	 * @throws InstantiationException
	 */
	public static <T> List<T> getResultBean(SolrClient client, T obj) throws IllegalAccessException, InstantiationException {
		Class clazz=obj.getClass();
		SolrQuery solrQuery = new SolrQuery();
		StringBuilder query = new StringBuilder();
		SolrDocumentList results=new SolrDocumentList();

		Field[] declaredFields = clazz.getDeclaredFields();
		try {
			for (Field field : declaredFields) {
				if (field.getName().equals("serialVersionUID")) {
					continue;
				}
				field.setAccessible(true);
				Object val = field.get(obj);
				if (val != null) {
					if (query.length() == 0) {
						query.append(field.getName()).append(":").append(val).append("\n");
					} else {
						query.append("AND").append("\n").append(field.getName()).append(":").append(val).append("\n");
					}
				}
			}
		} catch (IllegalAccessException e) {
			e.printStackTrace();
		}

		if (query.length()>0) {
			solrQuery.setQuery(query.toString());
			try {
				QueryResponse query1 = client.query(solrQuery);
				 results = query1.getResults();
				logger.info("查询内容:" + query.toString());
				logger.info("文档数量：" + results.getNumFound());
				logger.info("查询花费时间:" + query1.getQTime());
			} catch (SolrServerException | IOException e) {
				e.printStackTrace();
			}
		}
		List<T> objList=new ArrayList<>();
		if (!results.isEmpty() ){
			for (SolrDocument result : results) {
				Object o = clazz.newInstance();
				for (Field field : declaredFields) {
					if (field.getName().equals("serialVersionUID")) {
						continue;
					}
					String propertyName = field.getName();
					Object propertyValue = result.getFieldValue(propertyName);
					Class<?> propertyClass = field.getType();
					if(propertyClass.equals(Integer.class)) {
						Integer value = Integer.valueOf(String.valueOf(propertyValue));
						field.set(o, value);
					} else {
						field.set(o, propertyValue);
					}
				}
				objList.add((T)o);
			}
		}

		return objList;

	}



}
