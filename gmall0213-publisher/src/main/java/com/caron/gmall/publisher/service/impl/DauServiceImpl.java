package com.caron.gmall.publisher.service.impl;

import com.caron.gmall.publisher.service.DauService;
import com.google.gson.internal.$Gson$Preconditions;
import io.searchbox.client.JestClient;
import io.searchbox.core.Search;
import io.searchbox.core.SearchResult;
import io.searchbox.core.search.aggregation.TermsAggregation;
import org.elasticsearch.index.query.MatchAllQueryBuilder;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.bucket.terms.TermsBuilder;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @author Caron
 * @create 2020-07-24-11:20
 * @Description
 * @Version
 */
@Service
public class DauServiceImpl implements DauService {
    @Autowired
    JestClient jestClient;

    @Override
    public Long getDauTotal(String date) {
        String aliases = "gmall_dau_info0213_" +
                date.replace("-","") + "_query";
        // + "_query"
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        searchSourceBuilder.query(new MatchAllQueryBuilder());
        Search search = new Search.Builder(searchSourceBuilder.toString())
                .addIndex(aliases).addType("_doc").build();
        try {
            SearchResult serchResult = jestClient.execute(search);
            return serchResult.getTotal();
        }catch (IOException e){
            e.printStackTrace();
            throw new RuntimeException("es 查询异常");
        }
    }

    @Override
    public Map getDauHourCount(String date) {
        String aliases = "gmall_dau_info0213_" +
                date.replace("-","") + "_query";

        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        //分组统计
        TermsBuilder termsBuilder = AggregationBuilders
                .terms("groupby_hour").field("hr").size(24);

        searchSourceBuilder.aggregation(termsBuilder);
        Search search = new Search.Builder(searchSourceBuilder.toString())
                .addIndex(aliases).addType("_doc").build();
        try {
            SearchResult serchResult = jestClient.execute(search);
            Map resultMap = new HashMap();
            if(serchResult.getAggregations().getTermsAggregation("groupby_hour")!= null){
                List<TermsAggregation.Entry> buckets = serchResult.getAggregations().getTermsAggregation("groupby_hour").getBuckets();
                for(TermsAggregation.Entry bucket :buckets){
                    resultMap.put(bucket.getKey(),bucket.getCount());
                }
                return resultMap;
            }else{
                return new HashMap();
            }
        }catch (IOException e){
            e.printStackTrace();
            throw new RuntimeException("es 查询异常");
        }
    }
}
