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

public class AbortExecutor implements Callable<abort_result> {
  private Logger log = LoggerFactory.getLogger(this.getClass());
  private DMSiteManagerClient client;
  private int clientId;

  public AbortExecutor(DMSiteManagerClient client, int clientId) {
    this.client = client;
    this.clientId = clientId;
  }

  @Override
  public abort_result call() {
    return abort();
  }

  public abort_result abort() {
    abort_result res = new abort_result();
    res.status = exec_status_type.GENERIC_ERROR;
    if (client == null) {
      return res;
    }
    try {
      log.info("Executing abort");
      res = client.abortTransaction(clientId);
    } catch (TException x) {
      log.error("Error abortting transaction:{}", x.getMessage());
      log.error("trying to get stack trace", x);
	  res.status = exec_status_type.GENERIC_ERROR;
    }
    return res;
  }
}
