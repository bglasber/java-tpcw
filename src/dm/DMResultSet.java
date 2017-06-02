package dm;

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

public class DMResultSet {
  private Logger log = LoggerFactory.getLogger(this.getClass());

  private query_result query_res;
  private int cursor;

  public DMResultSet() {
	  this.query_res = new query_result();
	  query_res.ntups = -1;
	  cursor = -1;
  }

  public DMResultSet(query_result query_res) {
	this.query_res = query_res;
	cursor = -1;
  }

  public boolean append(DMResultSet dm_res_to_add) {
	  query_result res_to_add = dm_res_to_add.query_res;
	  if (query_res.ntups == -1) {
		query_res = res_to_add;
		return true;
	  }
	  if (res_to_add.ntups == 0) {
		return true;
	  }
	  if (query_res.numAttributes != res_to_add.numAttributes) {
		  return false;
	  }
	  query_res.tuples.addAll(res_to_add.tuples);
	  query_res.ntups = query_res.ntups + res_to_add.ntups;
	  query_res.numAffectedRows = query_res.numAffectedRows + res_to_add.numAffectedRows;
	  return true;
  }

  public void close() { cursor = query_res.ntups; }
  public boolean next() throws SQLException {
    cursor = cursor + 1;
    return (cursor < query_res.ntups);
  }


  public int getInt(int columnIndex) throws SQLException {
    String entry = getString(columnIndex);
    return Integer.parseInt(entry);
  }

  public String  getString(int columnIndex) throws SQLException {
    throwIfOutOfBounds(columnIndex);
    return query_res.tuples.get(cursor).get(columnIndex - 1);
  }

  private void throwIfOutOfBounds(int columnIndex) throws SQLException {
    if (cursor >= query_res.ntups) {
      throw new SQLException("Access cursor past number of tuples");
    }
    if (columnIndex > query_res.numAttributes) {
      throw new SQLException("Access column past number of attributes");
    }
  }
}

