package com.core.service;

import java.util.Map;
import java.util.Set;
import java.util.List;
import org.slf4j.Logger;
import java.util.Iterator;
import java.util.ArrayList;
import com.core.bean.PageBean;
import org.slf4j.LoggerFactory;
import com.alibaba.fastjson.JSON;
import java.net.UnknownHostException;
import org.elasticsearch.search.SearchHit;
import org.springframework.stereotype.Service;
import org.elasticsearch.search.sort.SortOrder;
import org.elasticsearch.search.sort.SortBuilders;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.TermQueryBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.index.query.RangeQueryBuilder;
import org.elasticsearch.index.query.MatchQueryBuilder;
import org.elasticsearch.client.transport.TransportClient;
import org.springframework.beans.factory.annotation.Value;
import org.elasticsearch.index.query.MultiMatchQueryBuilder;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.springframework.beans.factory.annotation.Autowired;



/**
 * Elastic Search 服务
 * 
 * @author zzj-zhang
 *
 */
@Service
public class ElasticSearchServiceImpl {

	// 日志
	Logger logger = LoggerFactory.getLogger(ElasticSearchServiceImpl.class);


	// 字段
	// Elastic Search 传输 客户端
	@Autowired
	private TransportClient transportClient;

	// Elastic Search 执行 超时 时间
	@Value("${elasticSearch.cluster.timeOutMillis}")
	private String timeOutMillis;


	// 方法
	/**
	 * 分页查询数据（交集 AND）
	 * 
	 * @param
	 * index
	 * type
	 * termQueryParams ( field : value ) - 精确匹配
	 * matchQueryParamList ( field : value ) - 拆词查询
	 * multiMatchQueryParams ( value : field set ) - 多匹配拆词查询
	 * rangeQueryParams ( field : value list )
	 * boolQueryBuilderList - 布尔查询
	 * sortFields
	 * pageNo
	 * pageSize
	 * clazz
	 * 
	 * Notes:
	 * 设置 查询时候 分词器 queryBuilder.analyzer("ik_smart");
	 * 需要显示的字段，逗号分隔（缺省为全部字段）searchRequestBuilder.setFetchSource(field, null);
	 * 对于termQuery进行完全匹配的字段，需要把字段类型设置为keyword
	 * 
	 * 设置是否按查询匹配度排序 
	 * 
	 */
	public <T> PageBean<T> pageSearch(String index, String type, 
			Map<String, Object> termQueryParams, 
			List<Map<String, Object>> matchQueryParamList,
			Map<String, Set<String>> multiMatchQueryParams, 
			Map<String, ArrayList<String>> rangeQueryParams, 
			List<BoolQueryBuilder> boolQueryBuilderList, 
			Set<String> sortFields, int pageNo, int pageSize, Class<T> clazz) throws UnknownHostException {

		// 创建查询构建者
		BoolQueryBuilder boolQueryBuilder = QueryBuilders.boolQuery();

		// 创建查询
		// 1) TermQueryBuilder
		if(termQueryParams != null && termQueryParams.size() > 0) {
			Set<String> keySet = termQueryParams.keySet();

			for(String key : keySet) {
				TermQueryBuilder termQueryBuilder = QueryBuilders.termQuery(key, termQueryParams.get(key));
				boolQueryBuilder.must(termQueryBuilder);
			}
		}

		// 2) MatchQueryBuilder
		if (matchQueryParamList != null && matchQueryParamList.size() > 0) {
			for(Map<String, Object> matchQueryParam : matchQueryParamList) {
				Set<String> keySet = matchQueryParam.keySet();

				for(String key : keySet) {
					MatchQueryBuilder matchQueryBuilder = QueryBuilders.matchQuery(key, matchQueryParam.get(key));
					boolQueryBuilder.must(matchQueryBuilder);
				}
			}
		}

		// 3) MultiMatchQueryBuilder
		if (multiMatchQueryParams != null && multiMatchQueryParams.size() > 0) {
			Set<String> keySet = multiMatchQueryParams.keySet();

			for(String key : keySet) {
				Set<String> fieldSet = multiMatchQueryParams.get(key);
				MultiMatchQueryBuilder multiMatchQueryBuilder = QueryBuilders.multiMatchQuery(key, fieldSet.toArray(new String[fieldSet.size()]));

				boolQueryBuilder.must(multiMatchQueryBuilder);
			}
		}

		// 4) RangeQueryBuilder
		if (rangeQueryParams != null && rangeQueryParams.size() > 0) {
			Set<String> keySet = rangeQueryParams.keySet();

			for(String key : keySet) {
				ArrayList<String> rangeValueList = rangeQueryParams.get(key);

				RangeQueryBuilder rangeQueryBuilder = QueryBuilders.rangeQuery(key);
				rangeQueryBuilder.from(rangeValueList.get(0), Boolean.TRUE);
				rangeQueryBuilder.to(rangeValueList.get(1), Boolean.TRUE);

				boolQueryBuilder.must(rangeQueryBuilder);
			}
		}

		// 5) BoolQueryBuilderList
		if (boolQueryBuilderList != null && boolQueryBuilderList.size() > 0) {
			for(BoolQueryBuilder queryBuilder : boolQueryBuilderList) {
				boolQueryBuilder.must(queryBuilder);
			}
		}

		// 创建查询请求
        SearchRequestBuilder searchRequestBuilder = transportClient.prepareSearch(index);

        searchRequestBuilder.setTypes(type);
        searchRequestBuilder.setSearchType(SearchType.QUERY_THEN_FETCH);

        // 排序字段
        // 先按 相似度 排序
        // searchRequestBuilder.addSort(SortBuilders.scoreSort().order(SortOrder.DESC));
        // searchRequestBuilder.setExplain(true);

        // 再按 字段 排序
        if (sortFields != null && sortFields.size() > 0) {
        	Iterator<String> iterator = sortFields.iterator();

        	while (iterator.hasNext()) {
        		searchRequestBuilder.addSort(iterator.next(), SortOrder.DESC);
        	}
        }

        // 设置 查询 条件
        searchRequestBuilder.setQuery(boolQueryBuilder);

        // 分页应用
        pageNo = ( pageNo == 0 ? 1 : pageNo );
        int pageFromIndex = ( pageNo - 1 ) * pageSize;
        searchRequestBuilder.setFrom(pageFromIndex).setSize(pageSize);

        // 执行搜索 返回响应
        SearchResponse searchResponse = searchRequestBuilder.execute().actionGet(Long.valueOf(timeOutMillis));

        // 处理响应
        // 所有 符合 查询 的 响应数量
        long totalHits = searchResponse.getHits().getTotalHits();

        // 符合 查询 分页 结果 的 响应数量
        long length = searchResponse.getHits().getHits().length;

        // 创建分页结果
        PageBean<T> pagingResult = new PageBean<T>();

        if (searchResponse.status().getStatus() == 200) {
            List<T> sourceList = new ArrayList<T>();

            for(SearchHit searchHit : searchResponse.getHits().getHits()) {
                T tInstance = JSON.parseObject(JSON.toJSONString(searchHit.getSourceAsMap()), clazz);
                sourceList.add(tInstance);
            }

            pagingResult.setCurrentPage(pageNo);
            pagingResult.setPageCount( (totalHits % pageSize) == 0 ? Integer.valueOf(String.valueOf(totalHits / pageSize)) : Integer.valueOf(String.valueOf((totalHits / pageSize) + 1)) );
            pagingResult.setPageSize(pageSize);
            pagingResult.setRecordList(sourceList);
            pagingResult.setRecordCount(Integer.valueOf(String.valueOf(totalHits)));
        }

        return pagingResult;
	}



}