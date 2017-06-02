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

public class PrepareExecutor implements Callable<commit_result> {
  private Logger log = LoggerFactory.getLogger(this.getClass());
  private DMSiteManagerClient client;
  private int clientId;

  public PrepareExecutor(DMSiteManagerClient client, int clientId) {
    this.client = client;
    this.clientId = clientId;
  }

  @Override
  public commit_result call() {
    return prepare();
  }

  public commit_result prepare() {
    commit_result res = new commit_result();
    res.status = exec_status_type.GENERIC_ERROR;
    if (client == null) {
      return res;
    }
    try {
      log.info("Executing prepare");
      res = client.prepareTransaction(clientId);
    } catch (TException x) {
      log.error("Error preparing transaction:{}", x.getMessage());
      log.error("trying to get stack trace", x);
	  res.status = exec_status_type.GENERIC_ERROR;
    }
    return res;
  }
}
