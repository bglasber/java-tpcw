package servlets;

import java.sql.SQLException;
import java.util.Vector;
import java.util.Date;
import java.util.Map;
import java.util.HashMap;
import java.util.Enumeration;
import java.text.SimpleDateFormat;

import dm.DMConn;

public class TPCW_DM {
  private static Map<Integer, DMConn> connMap = new HashMap<>();

  DMConn getConn(int eb_id) {
    DMConn conn = connMap.get(eb_id);
    if (conn == null) {
      connMap.put(eb_id, DMUtil.makeDMConnection(eb_id));
      conn = connMap.get(eb_id);
    }
	return conn;
  }
}
