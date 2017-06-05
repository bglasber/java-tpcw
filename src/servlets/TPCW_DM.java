package servlets;

import java.sql.SQLException;
import java.util.Vector;
import java.util.Date;
import java.util.Map;
import java.util.ArrayList;
import java.util.List;
import java.util.HashMap;
import java.util.Enumeration;
import java.text.SimpleDateFormat;

import dm.DMConn;
import dm.DMConnId;
import dm.DMResultSet;

import common.SQL;

import com.dynamic.mastering.primary_key;

public class TPCW_DM {
  private static Map<Integer, DMConn> connMap = new HashMap<>();

  public static DMConn getConn(int eb_id) {
    DMConn conn = connMap.get(eb_id);
    if (conn == null) {
      connMap.put(eb_id, DMUtil.makeDMConnection(eb_id));
      conn = connMap.get(eb_id);
    }
	return conn;
  }

  public static Book getBook(int eb_id, int i_id) {
	DMConn conn = getConn(eb_id);
    Book book = null;
    try {
	  conn.begin();
      book = getBookWithinTxn(eb_id, i_id);
	  conn.commit();
    } catch (SQLException e) {
      e.printStackTrace();
	  abort(eb_id);
    }

    return book;
  }

  public static Book getBookWithinTxn(int eb_id, int i_id) {
    DMConn conn = getConn(eb_id);
    Book book = null;
	String stmt = SQL.getBook;
    try {
      DMResultSet rs = conn.executeReadQuery(stmt, String.valueOf(i_id));
      book = new Book(rs);
    } catch (SQLException e) {
      e.printStackTrace();
	  abort(eb_id);
    }

    return book;
  }

  public static Map<primary_key, DMConnId> beginAdminResponse(int eb_id,
                                                              int i_id) {
    DMConn conn = getConn(eb_id);
    Map<primary_key, DMConnId> writeLocations = null;
    try {
      List<primary_key> keys = new ArrayList<primary_key>();
      keys.add(DMUtil.constructItemPrimaryKey(i_id));

	  return conn.begin(keys);

    } catch (SQLException e) {
      e.printStackTrace();
	  abort(eb_id);
    }
    return writeLocations;
  }

  public static void adminUpdate(int eb_id,
                                 Map<primary_key, DMConnId> writeLocations,
                                 int i_id, double cost, String image,
                                 String thumbnail) {
    DMConn conn = getConn(eb_id);
    try {

      primary_key pk = DMUtil.constructItemPrimaryKey(i_id);

      String adminQuery = conn.constructQuery(
          SQL.adminUpdate, String.valueOf(cost), "'" + image + "'",
          "'" + thumbnail + "'", String.valueOf(i_id));
      conn.executeWriteQuery(adminQuery, writeLocations.get(pk));

      DMResultSet rs = conn.executeReadQuery(
          SQL.adminUpdate_related, String.valueOf(i_id), String.valueOf(i_id));

      int[] related_items = new int[5];
      int counter = 0;
      int last = 0;

      while (rs.next()) {
        last = rs.getInt("ol_i_id");
        related_items[counter] = last;
        counter++;
      }

      for (int i = counter; i < 5; i++) {
        last++;
        related_items[i] = last;
      }

      String relatedQuery = conn.constructQuery(
          SQL.adminUpdate_related1, String.valueOf(related_items[0]),
          String.valueOf(related_items[1]), String.valueOf(related_items[2]),
          String.valueOf(related_items[3]), String.valueOf(related_items[4]),
          String.valueOf(i_id));
	  conn.executeWriteQuery(relatedQuery, writeLocations.get(pk));

    } catch (SQLException e) {
      e.printStackTrace();
	  abort(eb_id);
    }
  }

  public static void commit(int eb_id) {
	DMConn conn = getConn(eb_id);
	try {
      conn.commit();
    } catch (SQLException e) {
      e.printStackTrace();
	  abort(eb_id);
    }
  }
  public static void abort(int eb_id) {
	DMConn conn = getConn(eb_id);
	try {
      conn.abort();
    } catch (SQLException e) {
      e.printStackTrace();
    }
  }

}
