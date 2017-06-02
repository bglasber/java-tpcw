package dm;

import java.nio.ByteBuffer;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang3.StringUtils;

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

public class DMConn {
  private static Logger log  = LoggerFactory.getLogger(DMConn.class);
  private static final String SQL_VARIABLE = "?";
  private final DMClient client;

  public DMConn(DMClient client) {
    this.client = client;
  }

  public Map<primary_key, DMConnId> begin(List<primary_key> write_set) throws SQLException {
    log.info("calling begin");
    log.info("write_set:{}", write_set.toString());
	Map<primary_key, DMConnId> write_mapping = client.beginTransaction(write_set);
    throwIfNoSuccess((write_mapping != null), "Unable to begin transaction");
	return write_mapping;
  }

  public Map<primary_key, DMConnId> begin() throws SQLException {
    return begin(new ArrayList<primary_key>());
  }

  public void commit() throws SQLException {
    boolean success = client.commitTransaction();
    throwIfNoSuccess(success, "Unable to commit transaction");
  }

  public void abort() throws SQLException {
    boolean success = client.abortTransaction();
    throwIfNoSuccess(success, "Unable to abort transaction");
  }

  public void executeStoredProcedure(String name, Map<DMConnId, ByteBuffer> args)  throws SQLException {
    boolean success = client.storedProcedure(name, args);
    String exceptionString = "Unable to executeStoredProcedure:" + name;
    throwIfNoSuccess(success, exceptionString);
  }


  public void executeStoredProcedure(String name, ByteBuffer args)  throws SQLException {
    boolean success = client.storedProcedure(name, args);
    String exceptionString = "Unable to executeStoredProcedure:" + name;
    throwIfNoSuccess(success, exceptionString);
  }

  public DMResultSet executeReadQuery(Map<DMConnId, String> queries) throws SQLException {
	  DMResultSet baseRes = new DMResultSet();
	  List<query_result> results = client.readQuery(queries);
	  for (query_result query_res : results) {
		  baseRes.append(new DMResultSet(query_res));
	  }
	  return baseRes;
  }

  public DMResultSet executeReadQuery(String query, DMConnId connId)
      throws SQLException {
    query_result result = client.readQuery(query, connId);
    if (result == null) {
      String exceptionString = "Unable to executeReadQuery:" + query;
      throwIfNoSuccess(false, exceptionString);
    }
    return new DMResultSet(result);
  }

  public DMResultSet executeReadQuery(String sqlStringWithVariables,
                                      String... replacements)
      throws SQLException {
    String query = constructQuery(sqlStringWithVariables, replacements);
    query_result result = client.readQuery(query);
    if (result == null) {
      String exceptionString = "Unable to executeReadQuery:" + query;
      throwIfNoSuccess(false, exceptionString);
    }
    return new DMResultSet(result);
  }

  public Integer executeWriteQuery(Map<DMConnId, String> queries) throws SQLException {
	  List<query_result> results = client.writeQuery(queries);
	  return 0;
  }

  public Integer executeWriteQuery(String query, DMConnId connId) throws SQLException {
	  query_result result = client.writeQuery(query, connId);
	  if (result == null) {
		  String exceptionString = "Unable to executeWriteQuery:" + query;
		  throwIfNoSuccess(false, exceptionString);
    }
    return 0;

  }

  public Integer executeWriteQuery(String sqlStringWithVariables,
                                   String... replacements) throws SQLException {
    String query = constructQuery(sqlStringWithVariables, replacements);
    query_result result = client.writeQuery(query);
    if (result == null) {
      String exceptionString = "Unable to executeWriteQuery:" + query;
      throwIfNoSuccess(false, exceptionString);
    }
    return 0;
  }


  public static String constructQuery(String sqlStringWithVariables,
                                      String... replacements) {
    String query = sqlStringWithVariables;
    String sqlQuery = sqlStringWithVariables;

    for (int i = 0; i < replacements.length; i++) {
      query = StringUtils.replaceOnce(query, SQL_VARIABLE, replacements[i]);
    }
    return query;
  }

  public DMResultSet executeSingleReadQuery(String sqlStringWithVariables,
                                            String... replacements)
      throws SQLException {
    begin();
    DMResultSet res = executeReadQuery(sqlStringWithVariables, replacements);
    commit();
    return res;
  }

  public List<Integer> getClientSessionVersionVector() {
	  return client.getClientSessionVersionVector();
  }

  private void throwIfNoSuccess(boolean success, String exceptionStr)
      throws SQLException {
    if (!success) {
      log.warn(exceptionStr);
      throw new SQLException(exceptionStr);
    }
  }

  public void close() throws SQLException {
    boolean success = client.close();
    throwIfNoSuccess(success, "Unable to close connection");
  }

  public static DMClient constructDMClient(String connection, int clientId) {
    return constructDMClient(connection, clientId, new ArrayList<Integer>());
  }

  public static DMClient constructDMClient(String connection, int clientId,
                                           List<Integer> clientSessionVector) {
    DMClient constructedClient = null;
    try {
      String[] splitConnection = connection.split(":");
      String host = splitConnection[0];
      int port = Integer.parseInt(splitConnection[1]);

      log.info("Constructing client on host:{}, port:{}", host, port);

	  DMConnId connId = new DMConnId(host, port);

      constructedClient = new DMClient(connId, clientId, clientSessionVector);
    } catch (TException x) {
      log.error("Error constructing client:{}", x.getMessage());
      return null;
    }
    log.info("Returning client!");
    return constructedClient;
  }

  public static primary_key constructPrimaryKey(int tableId, long rowId) {
    return new primary_key(tableId, rowId);
  }
}
