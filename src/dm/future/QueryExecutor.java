package dm.future;

import java.util.concurrent.Callable;

import org.apache.thrift.TException;
import org.apache.thrift.transport.TSSLTransportFactory;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TSSLTransportFactory.TSSLTransportParameters;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import dm.DMSiteManagerClient;
import com.dynamic.mastering.*;

public class QueryExecutor implements Callable<query_result> {
  private Logger log = LoggerFactory.getLogger(this.getClass());
  private DMSiteManagerClient client;
  private int clientId;
  private boolean isReadQuery;
  private String query;

  public QueryExecutor(DMSiteManagerClient client, int clientId, boolean isReadQuery, String query) {
    this.client = client;
    this.clientId = clientId;
	this.isReadQuery = isReadQuery;
    this.query = query;
  }

  @Override
  public query_result call() {
    return query();
  }

  public query_result query() {
    query_result res = new query_result();
    res.status = exec_status_type.GENERIC_ERROR;
    if (client == null) {
      return res;
    }
    try {
      log.info("Executing query");
      log.info("Executing query:{}", query);
      if (isReadQuery) {
        res = client.select(clientId, query);
      } else {
        res = client.execute(clientId, query);
      }
    } catch (TException x) {
      log.error("Error with query:{}, {}", query, x.getMessage());
      log.error("trying to get stack trace", x);
      res.status = exec_status_type.GENERIC_ERROR;
    }
    return res;
  }
}
