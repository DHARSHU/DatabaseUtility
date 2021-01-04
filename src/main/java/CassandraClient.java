import com.datastax.driver.core.*;
import com.datastax.driver.core.policies.DCAwareRoundRobinPolicy;
import com.datastax.driver.core.policies.DefaultRetryPolicy;
import com.datastax.driver.core.policies.TokenAwarePolicy;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;


public class CassandraClient {
    private Session session;
    private Cluster cluster;
    private String contactPoints;
    private String username;
    private String password;
    private String keyspace;

    public CassandraClient(String contactPoints, String keyspace, String username, String password) {
        this.contactPoints = contactPoints;
        this.username = username;
        this.password = password;
        this.keyspace = keyspace;
    }

    public Cluster createCluster() {
        AuthProvider authProvider = new PlainTextAuthProvider(username, password);
        String[] contacts = contactPoints.split(",");
        cluster = Cluster.builder().addContactPoints(contacts).withRetryPolicy(DefaultRetryPolicy.INSTANCE).withLoadBalancingPolicy(new TokenAwarePolicy(new DCAwareRoundRobinPolicy.Builder().build())).withAuthProvider(authProvider).build();
        return cluster;
    }

    public Session createSession() {
        session = createCluster().connect(keyspace);
        return session;
    }

    public void closeConnection() {
        session.close();
        cluster.close();
    }

    public List<Map<String, Object>> execute(String query) {
        ResultSet resultSet = session.execute(query);
        return normalizeResults(resultSet);
    }

    public List<Map<String, Object>> executePreparedStatement(String query, String... values) {
        PreparedStatement preparedStatement = session.prepare(query);
        BoundStatement bound = preparedStatement.bind(values);
        ResultSet resultSet = session.execute(bound);
        return normalizeResults(resultSet);
    }

    public List<Map<String, Object>> executePreparedStatement(String query, List<String> values) {
        PreparedStatement preparedStatement = session.prepare(query);
        BoundStatement bound = preparedStatement.bind(values);
        ResultSet resultSet = session.execute(bound);
        return normalizeResults(resultSet);
    }

    public ResultSet executePreparedStatementReturnsResultset(String query, String... values) {
        PreparedStatement preparedStatement = session.prepare(query);
        BoundStatement bound = preparedStatement.bind(values);
        return session.execute(bound);
    }

    private List<Map<String, Object>> normalizeResults(ResultSet resultSet) {
        List<Map<String, Object>> list = new ArrayList<>();
        List<String> columnNames = getColumnNames(resultSet);
        for (Row row : resultSet) {
            Map<String, Object> map = new HashMap<>();
            columnNames.forEach(column -> {
                map.put(column, row.getObject(column));
            });
            list.add(map);
        }
        return list;
    }


    private List<String> getColumnNames(ResultSet resultSet) {
        return (resultSet.getColumnDefinitions().asList().stream()
                .map(ColumnDefinitions.Definition::getName).collect(Collectors.toList()));
    }
}