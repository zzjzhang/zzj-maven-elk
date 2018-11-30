package com.core.controller;

import java.util.Map;
import java.util.List;
import java.io.IOException;
import java.util.ArrayList;
import java.net.UnknownHostException;
import org.elasticsearch.common.text.Text;
import org.elasticsearch.search.SearchHit;
import org.springframework.http.HttpStatus;
import org.elasticsearch.client.AdminClient;
import org.elasticsearch.action.ActionFuture;
import org.springframework.http.ResponseEntity;
import org.elasticsearch.action.get.GetResponse;
import com.core.service.ElasticSearchServiceImpl;
import org.elasticsearch.client.IndicesAdminClient;
import org.elasticsearch.action.get.GetRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.client.transport.TransportClient;
import org.springframework.web.bind.annotation.GetMapping;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import org.elasticsearch.action.admin.indices.create.CreateIndexResponse;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexResponse;
import org.elasticsearch.search.fetch.subphase.highlight.HighlightBuilder;
import org.elasticsearch.action.admin.indices.mapping.put.PutMappingResponse;
import org.elasticsearch.action.admin.indices.exists.indices.IndicesExistsRequest;
import org.elasticsearch.action.admin.indices.exists.indices.IndicesExistsResponse;



@RestController
public class Controller {

	//1. 字段
	@Autowired
	private TransportClient transportClient;

	@Autowired
	private ElasticSearchServiceImpl elasticSearchServiceImpl;



	/**
	 * 创建 空索引
	 * 
	 * indices 就是索引
	 * 
	 */
	@GetMapping("/createIndex")
	@ResponseBody
	public boolean createIndex(@RequestParam(name = "index") String index) throws IOException {
		// 1. 创建客户端
		AdminClient adminClient = transportClient.admin();
		IndicesAdminClient indicesAdminClient = adminClient.indices();

		// 2. 创建请求
		IndicesExistsRequest indicesExistsRequest = new IndicesExistsRequest(index);

		ActionFuture<IndicesExistsResponse> actionFutureExists = indicesAdminClient.exists(indicesExistsRequest);
		IndicesExistsResponse indicesExistsResponse = actionFutureExists.actionGet();

		if(indicesExistsResponse.isExists()) {
			System.out.println("索引已经存在...");
			return Boolean.FALSE;
		} else {
			System.out.println("索引不存在...");

			//CreateIndexRequestBuilder createIndexRequestBuilder = indicesAdminClient.prepareCreate(indexName);
			//ActionFuture<CreateIndexResponse> actionFutureCreate = createIndexRequestBuilder.execute();
			//CreateIndexResponse createIndexResponse = actionFutureCreate.actionGet();
			CreateIndexResponse createIndexResponse = indicesAdminClient.prepareCreate(index).execute().actionGet(60 * 1000);
			return createIndexResponse.isAcknowledged();
		}
	}

	
	
	
	
	/**
	 * 为空索引添加映射
	 * 
	 * 设置映射API允许我们在指定索引上一次性创建或修改一到多个索引的映射。设置映射必须确保指定的索引存在，否则会报错。
	 * 
	 */
	@GetMapping("/putMapping")
	@ResponseBody
	public boolean putMapping(@RequestParam(name = "index") String index, @RequestParam(name = "type") String type) throws Exception {
		// mapping
		XContentBuilder mappingBuilder;

		try {
			mappingBuilder = XContentFactory.jsonBuilder();

			mappingBuilder.startObject();
			mappingBuilder.startObject(type);
			mappingBuilder.startObject("properties");

			//
			mappingBuilder.startObject("id").field("type", "keyword").field("store", Boolean.TRUE).endObject()
		    .startObject("res_no").field("type", "text").field("store", Boolean.TRUE).endObject()

		    .startObject("title").field("type", "text")
		    .field("analyzer", "ik_max_word")
		    .field("search_analyzer", "ik_max_word")
		    .field("store", Boolean.TRUE)
		    .endObject()

		    .startObject("cover_pic_id").field("type", "long").field("store", Boolean.TRUE).endObject()
		    .startObject("res_attr").field("type", "text").field("store", Boolean.TRUE).field("fielddata", Boolean.TRUE).endObject()
		    .startObject("comment_count").field("type", "short").field("store", Boolean.TRUE).endObject()
		    .startObject("release_time").field("type", "date").field("store", Boolean.TRUE).endObject();

			//
			mappingBuilder.endObject();
			mappingBuilder.endObject();
			mappingBuilder.endObject();
		} catch (Exception e) {
			System.out.println("--------- createIndex 创建 mapping 失败：");
			return false;
		}

		AdminClient adminClient = transportClient.admin();
		IndicesAdminClient indicesAdminClient = adminClient.indices();
		PutMappingResponse response = indicesAdminClient.preparePutMapping(index).setType(type).setSource(mappingBuilder).execute().actionGet(60 * 1000);

		return response.isAcknowledged();
	}



	/**
	 * 删除索引
	 * 
	 * @param
	 * index: 索引名称
	 * 
	 */
	@GetMapping(value = "/removeIndex")
	@ResponseBody
	public boolean removeIndex(@RequestParam(name = "index") String index) throws UnknownHostException {
		// 1. 创建客户端
		AdminClient adminClient = transportClient.admin();
		IndicesAdminClient indicesAdminClient = adminClient.indices();
		IndicesExistsRequest indicesExistsRequest = new IndicesExistsRequest(index);

		ActionFuture<IndicesExistsResponse> actionFutureExists = indicesAdminClient.exists(indicesExistsRequest);
		IndicesExistsResponse indicesExistsResponse = actionFutureExists.actionGet();

		if(indicesExistsResponse.isExists()) {
			System.out.println("索引存在，能够删除...");
			DeleteIndexResponse response = indicesAdminClient.prepareDelete(index).execute().actionGet(60 * 1000);
			return response.isAcknowledged();
		} else {
			System.out.println("索引不存在，无法删除...");
			return Boolean.FALSE;
		}
	}



	/**
	 * 
	 * 创建复杂索引
	 * 
	 * 下面代码创建复杂索引，给它设置它的映射(mapping)和设置信息(settings)，指定分片个数为3，副本个数为2，同时设置school字段不分词。
	 * 
	 */
	/*public static boolean createIndex(Client client, String index) {
		// settings
		Settings settings = Settings.builder().put("index.number_of_shards", 3).put("index.number_of_replicas", 2).build();

		// mapping
		XContentBuilder mappingBuilder;

		try {
			mappingBuilder = XContentFactory.jsonBuilder()
							.startObject()
							.startObject(index)
							.startObject("properties")
							.startObject("name").field("type", "string").field("store", true).endObject()
							.startObject("sex").field("type", "string").field("store", true).endObject()
							.startObject("college").field("type", "string").field("store", true).endObject()
							.startObject("age").field("type", "integer").field("store", true).endObject()
							.startObject("school").field("type", "string").field("store", true).field("index", "not_analyzed").endObject()
							.endObject()
							.endObject()
							.endObject();
		} catch (Exception e) {
			logger.error("--------- createIndex 创建 mapping 失败：",e);
			return false;
		}

		IndicesAdminClient indicesAdminClient = client.admin().indices();
		CreateIndexResponse response = indicesAdminClient.prepareCreate(index)
		.setSettings(settings)
		.addMapping(index, mappingBuilder)
		.get();
		return response.isAcknowledged();

	}*/



	/**
	 * 根据ID查询数据
	 * 
	 * @throws UnknownHostException 
	 * 
	 */
	@GetMapping("/getById")
	@ResponseBody
	public ResponseEntity getById(@RequestParam(name = "index") String index, @RequestParam(name = "type") String type, @RequestParam(name = "id") String id) throws UnknownHostException {
		GetRequestBuilder getRequestBuilder = transportClient.prepareGet(index, type, id);
		GetResponse getResponse = getRequestBuilder.get();

		if(!getResponse.isExists()) {
			return new ResponseEntity(getResponse, HttpStatus.NOT_FOUND);
		}

		return new ResponseEntity(getResponse, HttpStatus.OK);
	}

    
	
	/**
     * 
     * 按条件执行查询
     * 
     */
	@RequestMapping("queryElasticSearch")
	@ResponseBody
    public SearchResponse queryElasticSearch(String highlightField, int pageNo, int pageSize) {
    	SearchRequestBuilder searchRequestBuilder = transportClient.prepareSearch(null);
    	
    	// 执行查询
    	try {
			elasticSearchServiceImpl.pageSearch(null, null, null, null, null, null, null, null, 0, 0, null);
		} catch (UnknownHostException e) {
			e.printStackTrace();
		}

        // 高亮（xxx=111, aaa=222）
        if (highlightField != null) {
            HighlightBuilder highlightBuilder = new HighlightBuilder();

            highlightBuilder.preTags("<span style='color:red' >");     //设置前缀
            highlightBuilder.postTags("</span>");                      //设置后缀

            // 设置高亮字段
            highlightBuilder.field(highlightField);
            searchRequestBuilder.highlighter(highlightBuilder);
        }

        //
        searchRequestBuilder.setQuery(null);

        // 分页应用
        searchRequestBuilder.setFrom((pageNo - 1) * pageSize).setSize(pageSize);

        // 设置是否按查询匹配度排序
        searchRequestBuilder.setExplain(Boolean.TRUE);

        // 执行搜索, 返回搜索响应信息
        SearchResponse searchResponse = searchRequestBuilder.execute().actionGet(60 * 1000);

        // 返回查询结果
        return searchResponse;
    }
    
    

    
    
    /**
     * 高亮结果集 特殊处理
     *
     */
    private static List<Map<String, Object>> setSearchResponse(SearchResponse searchResponse, String highlightField) {
        List<Map<String, Object>> sourceList = new ArrayList<Map<String, Object>>();
        StringBuffer stringBuffer = new StringBuffer();

        for(SearchHit searchHit : searchResponse.getHits().getHits()) {
            searchHit.getSourceAsMap().put("id", searchHit.getId());

            if (highlightField != null) {
                System.out.println("遍历 高亮结果集，覆盖 正常结果集" + searchHit.getSourceAsMap());
                Text[] text = searchHit.getHighlightFields().get(highlightField).getFragments();
                if (text != null) {
                	for (Text str : text) {
                        stringBuffer.append(str.string());
                    }

                	//遍历 高亮结果集，覆盖 正常结果集
                    searchHit.getSourceAsMap().put(highlightField, stringBuffer.toString());
                }
            }

            sourceList.add(searchHit.getSourceAsMap());
        }

        return sourceList;
    }



}