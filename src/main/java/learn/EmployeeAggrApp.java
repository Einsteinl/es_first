package learn;

import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.search.aggregations.Aggregation;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.bucket.histogram.DateHistogramInterval;
import org.elasticsearch.search.aggregations.bucket.histogram.Histogram;
import org.elasticsearch.search.aggregations.bucket.terms.StringTerms;
import org.elasticsearch.search.aggregations.metrics.avg.Avg;
import org.elasticsearch.transport.client.PreBuiltTransportClient;
import org.elasticsearch.search.aggregations.bucket.terms.Terms.Bucket;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Iterator;
import java.util.Map;

/**
 * 员工聚合分析应用程序
 * 对字段类型为string进行聚合的时候需要指定的mapping fielddata=true，打开正排索引
 *
 * 需求：按照国家分组再按照入职日期分组求工资的平均值
 */

/**
 * GET /company/employee/_search
 * {
 *   "size": 0,
 *   "aggs":{
 *     "group_by_country":{
 *       "terms": {
 *         "field": "country"
 *       },
 *
 *     "aggs" :{
 *       "group_by_join_date":{
 *         "date_histogram": {
 *           "field": "join_date",
 *           "interval": "year"
 *         },
 *
 *
 *         "aggs": {
 *           "avg_salary": {
 *             "avg": {
 *               "field": "salary"
 *             }
 *           }
 *         }
 *         }
 *       }
 *     }
 *   }
 * }
 */

public class EmployeeAggrApp {
    public static void main(String[] args) throws UnknownHostException {
        Settings settings=Settings.builder()
                .put("cluster.name","elasticsearch")
                .build();
        TransportClient client=new PreBuiltTransportClient(settings)
                .addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName("localhost"),9300));

        //构建聚合搜索
        SearchResponse searchResponse = client.prepareSearch("company")
                .addAggregation(AggregationBuilders.terms("group_by_country").field("country")
                        .subAggregation(AggregationBuilders.dateHistogram("group_by_join_date")
                                .field("join_date")
                                .dateHistogramInterval(DateHistogramInterval.YEAR)
                                .subAggregation(AggregationBuilders.avg("avg_salary")
                                        .field("salary")))
                ).execute().actionGet();
        //TODO 拿到结果，迭代处理
        Map<String,Aggregation> aggrMap= searchResponse.getAggregations().asMap();
        StringTerms groupByCountry = (StringTerms) aggrMap.get("group_by_country");
        Iterator<Bucket> groupByCountryBucketIterator=groupByCountry.getBuckets().iterator();
        //首先拿最外层的国家聚合
        while (groupByCountryBucketIterator.hasNext()){
            Bucket groupByContryBucket=groupByCountryBucketIterator.next();
            System.out.println(groupByContryBucket.getKey()+":"+groupByContryBucket.getDocCount());
            //接着拿group_by_join_date聚合结果
            Histogram groupByJoinDate=(Histogram) groupByContryBucket.getAggregations().asMap().get("group_by_join_date");
            Iterator<Histogram.Bucket> groupByJoinDateBucketIterator = groupByJoinDate.getBuckets().iterator();
            while (groupByJoinDateBucketIterator.hasNext()){
                Histogram.Bucket groupByJoinDateBucket =groupByJoinDateBucketIterator.next();
                System.out.println(groupByJoinDateBucket.getKey()+":"+groupByJoinDateBucket.getDocCount());
                //最后，直接取出平均数
                Avg avg = (Avg) groupByJoinDateBucket.getAggregations().asMap().get("avg_salary");
                System.out.println(avg.getValue());
            }
        }

        client.close();
    }

}
