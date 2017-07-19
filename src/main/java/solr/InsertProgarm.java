package solr;

/**
 * @Name solr.InsertProgarm
 * @Description
 * @Author Elwyn
 * @Version 2017/7/18
 * @Copyright 上海云辰信息科技有限公司
 **/

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.HttpSolrClient;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.SolrDocumentList;
import org.apache.solr.common.SolrInputDocument;

public class InsertProgarm {
	//solr 服务器地址
	public static final String solrServerUrl = "http://localhost:8080/solr";
	//solrhome下的core
	public static final String solrCroeHome = "my_solr";
	//待索引、查询字段
	public static String[] docs = {"Solr是一个独立的企业级搜索应用服务器",
			"它对外提供类似于Web-service的API接口",
			"用户可以通过http请求",
			"向搜索引擎服务器提交一定格式的XML文件生成索引",
			"也可以通过Http Get操作提出查找请求",
			"并得到XML格式的返回结果"};

	public static SolrClient getSolrClient() {
		//return new HttpSolrClient(solrServerUrl + "/" + solrCroeHome);
		return  new HttpSolrClient.Builder(solrServerUrl + "/" + solrCroeHome).build();
	}


	/**
	 * 新建索引
	 * <p>
	 * 就是 把数据放到 solr中  以便搜索查询  可以单独写一个方法 导入
	 * <p>
	 * 最好是  配置solr 连接数据库  自动导入相关数据到索引库（没有配置好 如果你有完整的配置 方法  欢迎留言）
	 */

	public static void createIndex() {

		SolrClient client = getSolrClient();

		int i = 0;

		List<SolrInputDocument> docList = new ArrayList<SolrInputDocument>();

		for (String content : docs) {

			SolrInputDocument doc = new SolrInputDocument();

			doc.addField("id", i++);
			doc.addField("content_test", content);

			docList.add(doc);

		}

		try {

			client.add(docList);

			client.commit();

		} catch (SolrServerException e) {

			e.printStackTrace();

		} catch (IOException e) {

			e.printStackTrace();

		}

	}

	;


	/**
	 * 搜索
	 */

	public static void search() {

		SolrClient client = getSolrClient();

		SolrQuery query = new SolrQuery();

		//query.set("q","url:baidu");
		query.setQuery("*:*");
		query.setRows(1000);
		QueryResponse response = null;

		try {
			response = client.query(query);
			System.out.println(response.toString());
			SolrDocumentList docs = response.getResults();
			System.out.println("文档个数：" + docs.getNumFound());
			System.out.println("查询时间：" + response.getQTime());
			for (SolrDocument doc : docs) {
				System.out.println("id: " + doc.getFieldValue("id") + "      content: " + doc.getFieldValue("url"));
			}
		} catch (SolrServerException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}


	public static void main(String[] args) throws InstantiationException, IllegalAccessException {
		//search();

		/*SolrDocumentList baidu = new SolerServiceImpl().query("*:*");
		for (SolrDocument entries : baidu) {
			System.out.println(entries);
		}
*/
		Content content=new Content();
		content.setUrl("baidu");
		content.setService_code("14012210009567");

		List<Content> resultBean = SolrUtils.getResultBean(getSolrClient(), content);
		for (Content content1 : resultBean) {
			System.out.println(content1);
		}
	}
}