
import org.neo4j.driver.v1.*;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class Neo4jClient {
    private Session session;
    private String url, username, password;

    public Neo4jClient() {
    }

    public Neo4jClient(String url, String username, String password) {
        this.url = url;
        this.username = username;
        this.password = password;
        this.session = createSession();
    }

    public Session createSession() {
        Driver driver = GraphDatabase.driver(url, AuthTokens.basic(username, password));
        return driver.session();
    }

    public Session createSession(String url, String username, String password) {
        this.url = url;
        this.username = username;
        this.password = password;
        return createSession();
    }

    public List<String> getResultAsList(String statement, Map<String, Object> args) {
        List<String> list = new ArrayList<>();
        StatementResult result = session.run(statement, args);
        while (result.hasNext()) {
            Record record = result.next();
            String entry = String.valueOf(record.get(0));
            list.add(entry.substring(1, entry.length() - 1));
        }
        return list;
    }
}
