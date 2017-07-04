package dm;

import java.io.IOException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

import org.apache.thrift.TException;
import org.apache.thrift.transport.TSSLTransportFactory;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TSSLTransportFactory.TSSLTransportParameters;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.dynamic.mastering.*;

public class DMSiteSelectorClient {
  private Logger log = LoggerFactory.getLogger(this.getClass());

  TTransport transport;
  TProtocol protocol;
  SiteSelector.Client client;

  public DMSiteSelectorClient(DMConnId connId)
      throws TException {
    transport = new TSocket(connId.host, connId.port);
    transport.open();

    protocol = new TBinaryProtocol(transport);
    client = new SiteSelector.Client(protocol);
    log.info("Connecting to site selector OK!");
  }
  public List<site_selector_begin_result>
  beginTransaction(int id, List<Integer> client_session_version_vector,
                   List<com.dynamic.mastering.primary_key> write_set)
      throws TException {
    return client.rpc_begin_transaction(id, client_session_version_vector,
                                        write_set);
  }

  void close() throws TException { transport.close(); }
}
