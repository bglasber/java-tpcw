package dm.future;

import java.nio.ByteBuffer;
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

public class StoredProcedureExecutor implements Callable<sproc_result> {
  private Logger log = LoggerFactory.getLogger(this.getClass());
  private DMSiteManagerClient client;
  private int clientId;
  private String storedProcName;
  private ByteBuffer args;

  public StoredProcedureExecutor(DMSiteManagerClient client, int clientId, String storedProcName, ByteBuffer args) {
    this.client = client;
    this.clientId = clientId;
	this.storedProcName = storedProcName;
    this.args = args;
  }

  @Override
  public sproc_result call() {
    return storedProcedure();
  }

  public sproc_result storedProcedure() {
    sproc_result res = new sproc_result();
    res.status = exec_status_type.GENERIC_ERROR;
    if (client == null) {
      return res;
    }
    try {
      log.info("Executing sproc:{}", storedProcName);
      res = client.storedProcedure(clientId, storedProcName, args);

    } catch (TException x) {
      log.error("Error with storedProcedure:{}, {}", storedProcName, x.getMessage());
      log.error("trying to get stack trace", x);
      res.status = exec_status_type.GENERIC_ERROR;
    }
    return res;
  }
}
