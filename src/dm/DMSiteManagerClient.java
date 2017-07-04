package dm;

import java.io.IOException;
import java.nio.ByteBuffer;
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

import com.dynamic.mastering.*;

public class DMSiteManagerClient {
  TTransport transport;
  TProtocol protocol;
  site_manager.Client client;

  public DMSiteManagerClient(DMConnId connId)
      throws TException {
    transport = new TSocket(connId.host, connId.port);
    transport.open();

    protocol = new TBinaryProtocol(transport);
    client = new site_manager.Client(protocol);
  }

  public commit_result  prepareTransaction(int clientId) throws TException {
    return client.rpc_prepare_transaction(clientId);
  }

  public commit_result commitTransaction(int clientId) throws TException {
    return client.rpc_commit_transaction(clientId);
  }

  public abort_result abortTransaction(int clientId) throws TException {
    return client.rpc_abort_transaction(clientId);
  }

  public query_result select(int clientId, String query) throws TException {
    return client.rpc_select(clientId, query);
  }

  public query_result execute(int clientId, String query) throws TException {
    return client.rpc_execute(clientId, query);
  }

  public sproc_result storedProcedure(int clientId, String name, ByteBuffer args) throws TException {
    return client.rpc_stored_procedure(clientId, name, args);
  }

  void close() throws TException { transport.close(); }
}
