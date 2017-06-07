package dm;

import java.sql.Date;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

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
  private Map<String, Integer> labelToColumnMap;
  private int cursor;

  public DMResultSet() {
	  this.query_res = new query_result();
	  query_res.ntups = -1;
	  cursor = -1;
	  labelToColumnMap = new HashMap<String, Integer>();
  }

  public DMResultSet(query_result query_res) {
	this.query_res = query_res;
	cursor = -1;
	labelToColumnMap = new HashMap<String, Integer>();
	filLabelToColumnMap();
  }

  public boolean append(DMResultSet dm_res_to_add) {
	  query_result res_to_add = dm_res_to_add.query_res;
	  if (query_res.ntups == -1) {
		query_res = res_to_add;
		filLabelToColumnMap();
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
	log.info("cursor is now at: {} ntups is: {}", cursor, query_res.ntups);
    return (cursor < query_res.ntups);
  }

  public Date getDate(String columnLabel) throws SQLException {
    String entry = getString(columnLabel);
    return Date.valueOf(entry);
  }

  public double getDouble(String columnLabel) throws SQLException {
    String entry = getString(columnLabel);
    return Double.parseDouble(entry);
  }

  public int getInt(String columnLabel) throws SQLException {
    String entry = getString(columnLabel);
    return Integer.parseInt(entry);
  }

  public String getString(String columnLabel) throws SQLException {
	  Integer columnIndex = labelToColumnMap.get(columnLabel);

	  if (columnIndex == null) {
		  log.info("cannot find column:{}", columnLabel);
		  return null;
	  }
	  return getString(columnIndex);
  }

  public int getInt(int columnIndex) throws SQLException {
    String entry = getString(columnIndex);
    return Integer.parseInt(entry);
  }

  public String  getString(int columnIndex) throws SQLException {
    throwIfOutOfBounds(columnIndex);
    String res =  query_res.tuples.get(cursor).get(columnIndex - 1);
	log.info("got res:{}", res);
	return res;
  }

  private void throwIfOutOfBounds(int columnIndex) throws SQLException {
    if (cursor >= query_res.ntups) {
      throw new SQLException("Access cursor past number of tuples");
    }
    if (columnIndex > query_res.numAttributes) {
      throw new SQLException("Access column past number of attributes");
    }
  }

  private void filLabelToColumnMap() {
	  // this is some bull shit indexing by the get string command
	  int col = 1;
	  for (att_desc desc : query_res.attDescs) {
		  labelToColumnMap.put(desc.name, col);
		  log.info("column label:{}, val:{}", desc.name, col);
		  col++;
	  }
  }
}

