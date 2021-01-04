import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthRequest;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthResponse;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.indices.GetIndexRequest;
import org.elasticsearch.client.indices.PutMappingRequest;
import org.elasticsearch.index.query.*;
import org.elasticsearch.index.reindex.BulkByScrollResponse;
import org.elasticsearch.index.reindex.DeleteByQueryRequest;
import org.elasticsearch.search.builder.SearchSourceBuilder;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class ElasticClient {

    public String HOST;
    public int PORT;
    public RestHighLevelClient elasticRestClient;

    public ElasticClient(String host, String port, String scheme, String username, String password) {
        this.HOST = host;
        this.PORT = Integer.parseInt(port);
        CredentialsProvider credentialsProvider = new BasicCredentialsProvider();
        credentialsProvider.setCredentials(AuthScope.ANY, new UsernamePasswordCredentials(username, password));
        RestClientBuilder restClientBuilder = RestClient.builder(new HttpHost(HOST, PORT, scheme)).setHttpClientConfigCallback(httpClientBuilder -> httpClientBuilder.setDefaultCredentialsProvider(credentialsProvider));
        this.elasticRestClient = new RestHighLevelClient(restClientBuilder);
    }

    public ElasticClient(String host, String port, String scheme) throws IOException {
        this.HOST = host;
        this.PORT = Integer.parseInt(port);
        RestClientBuilder restClientBuilder = RestClient.builder(new HttpHost(HOST, PORT, scheme));
        this.elasticRestClient = new RestHighLevelClient(restClientBuilder);
    }

    public ClusterHealthResponse getElasticSearchHealth() {
        try {
            ClusterHealthRequest request = new ClusterHealthRequest();
            return elasticRestClient.cluster().health(request, RequestOptions.DEFAULT);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public GetResponse get(String index, String id) {
        try {
            GetRequest request = new GetRequest(index, id);
            return elasticRestClient.get(request, RequestOptions.DEFAULT);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public DeleteResponse delete(String index, String id) throws IOException {
        try {
            DeleteRequest request = new DeleteRequest(index, id);

            return elasticRestClient.delete(request, RequestOptions.DEFAULT);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public BulkByScrollResponse BulkByScrollResponse(String index, String field, String value) throws IOException {
        try {
            DeleteByQueryRequest request = new DeleteByQueryRequest(index).setQuery(new TermQueryBuilder(field, value));
            return elasticRestClient.deleteByQuery(request, RequestOptions.DEFAULT);


        } catch (IOException e) {
            new RuntimeException(e);
        }
        return null;
    }

    public SearchResponse searchByIndex(String index) throws IOException {
        try {
            SearchRequest request = new SearchRequest(index);
            return elasticRestClient.search(request, RequestOptions.DEFAULT);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public SearchResponse searchByTermQuery(String index, String key, String value) {
        try {
            SearchRequest request = new SearchRequest(index).source(new SearchSourceBuilder().query(QueryBuilders.termQuery(key, value)));
            return elasticRestClient.search(request, RequestOptions.DEFAULT);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public SearchResponse searchByMultipleValuesQuery(String index, Map<String, String> values) throws IOException {
        try {
            ArrayList<QueryBuilder> shouldList = new ArrayList<>();
            values.keySet().stream().forEach(key -> shouldList.add(new MatchQueryBuilder(key, values.get(key))));
            BoolQueryBuilder queryBuilder = new BoolQueryBuilder();
            queryBuilder.must().addAll(shouldList);
            SearchRequest request = new SearchRequest(index).source(new SearchSourceBuilder().query(queryBuilder));
            return elasticRestClient.search(request, RequestOptions.DEFAULT);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public SearchResponse SearchByShouldMustAndMustNotNull(String index, Map<String, String> mustValues, Map<String, String> shouldValues, Map<String, String> mustNotNullValues) throws IOException {
        try {
            BoolQueryBuilder boolQueryBuilder = new BoolQueryBuilder();
            mustValues.keySet().forEach(key -> boolQueryBuilder.must(new MatchQueryBuilder(key, mustValues.get(key))));
            shouldValues.keySet().stream().forEach(key -> boolQueryBuilder.should(new TermQueryBuilder(key, shouldValues.get(key))));
            mustNotNullValues.keySet().stream().forEach(key -> boolQueryBuilder.must(new ExistsQueryBuilder(key)));

            SearchRequest request = new SearchRequest(index).source(new SearchSourceBuilder().query(boolQueryBuilder));
            return elasticRestClient.search(request, RequestOptions.DEFAULT);

        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public IndexResponse postDocument(String Index, Map<String, String> Document) {
        try { IndexRequest request = new IndexRequest(Index); request.source(Document);
            return elasticRestClient.index(request, RequestOptions.DEFAULT);
    } catch (IOException e) {
        throw new RuntimeException(e);
    }}
    public BulkResponse postBulkDocument(String Index, List<Map<String, String>> Documents) throws IOException {
        try { BulkRequest request = new BulkRequest();
            Documents.stream().forEach(doc -> request.add(new IndexRequest(Index).source(doc)));
        return elasticRestClient.bulk(request, RequestOptions.DEFAULT); }
        catch (IOException e) {
            throw new RuntimeException(e); } }

    public String[] getIndicies() {
        try {
            GetIndexRequest request = new GetIndexRequest("*");
            return elasticRestClient.indices().get(request, RequestOptions.DEFAULT).getIndices();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public AcknowledgedResponse putIndicies(String Index, Map<String, String> Mappingsource) {
        try {
            PutMappingRequest request = new PutMappingRequest(Index);
            request.source(Mappingsource);
            return elasticRestClient.indices().putMapping(request, RequestOptions.DEFAULT);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public void close() throws IOException{
        elasticRestClient.close();
    }

}
