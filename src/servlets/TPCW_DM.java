package servlets;

import java.sql.SQLException;
import java.util.Vector;
import java.util.Date;
import java.util.Map;
import java.util.ArrayList;
import java.util.List;
import java.util.HashMap;
import java.util.Enumeration;

import java.util.concurrent.atomic.AtomicInteger;

import java.text.SimpleDateFormat;

import dm.DMConn;
import dm.DMConnId;
import dm.DMResultSet;

import common.SQL;

import com.dynamic.mastering.primary_key;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TPCW_DM {
  private static Logger log = LoggerFactory.getLogger(TPCW_DM.class);
  private static Map<Integer, DMConn> connMap = new HashMap<>();

  // 0 means no initialization has occurred
  // 1 means someone is initializing now
  // 2 means all initialization is complete
  private static AtomicInteger initializationStage = new AtomicInteger();
  private static Integer synchronizationPoint = new Integer(0);

  // counters for id's
  private static AtomicInteger addressCounter = new AtomicInteger();
  private static AtomicInteger orderCounter = new AtomicInteger();
  private static AtomicInteger customerCounter = new AtomicInteger();
  private static AtomicInteger shoppingCartCounter = new AtomicInteger();
  private static AtomicInteger shoppingCartLineCounter = new AtomicInteger();

  public static DMConn getConn(int eb_id) {
	log.info("Getting conn for eb_id:{}", eb_id);
    DMConn conn = connMap.get(eb_id);
    if (conn == null) {
      connMap.put(eb_id, DMUtil.makeDMConnection(eb_id));
      conn = connMap.get(eb_id);
    }
	log.info("Got conn for eb_id:{}", eb_id);
    return conn;
  }

  public static Book getBook(int eb_id, int i_id) {
    Book book = null;
    begin(eb_id);
    book = getBookWithinTxn(eb_id, i_id);
    commit(eb_id);

    return book;
  }

  public static Book getBookWithinTxn(int eb_id, int i_id) {
    DMConn conn = getConn(eb_id);
    Book book = null;
    String stmt = SQL.getBook;
    try {
      DMResultSet rs = conn.executeReadQuery(stmt, String.valueOf(i_id));
      if (rs.next()) {
        book = new Book(rs);
      }
      rs.close();
    } catch (SQLException e) {
      e.printStackTrace();
      abort(eb_id);
    }

    return book;
  }

  public static BeginShoppingCartResult
  beginShoppingCartWithEmptyCart(int eb_id) {
    int shoppingId = shoppingCartCounter.incrementAndGet();
    Map<primary_key, DMConnId> writeLocations =
        beginShoppingCart(eb_id, shoppingId);
    BeginShoppingCartResult res =
        new BeginShoppingCartResult(writeLocations, shoppingId);
    return res;
  }

  public static Map<primary_key, DMConnId> beginShoppingCart(int eb_id,
                                                             int shoppingId) {
    List<primary_key> keys = new ArrayList<primary_key>();
    keys.add(DMUtil.constructShoppingCartPrimaryKey(shoppingId));
    keys.add(DMUtil.constructShoppingCartLinePrimaryKey(shoppingId));

    return begin(eb_id, keys);
  }

  public static int
  createEmptyCartWithinTxn(int eb_id, Map<primary_key, DMConnId> writeLocations,
                           int shoppingId) {

    DMConn conn = getConn(eb_id);

    int SHOPPING_ID = shoppingId;
    try {
      String maxStmt = SQL.createEmptyCart;
      DMResultSet rs = conn.executeReadQuery(maxStmt);

      if (rs.next()) {
        int max_shopping_id = rs.getInt("COUNT(*)");
      }
      rs.close();

      primary_key pk = DMUtil.constructShoppingCartPrimaryKey(SHOPPING_ID);
      String stmt = SQL.createEmptyCart_insert;
      String query = conn.constructQuery(stmt, String.valueOf(SHOPPING_ID));
      conn.executeWriteQuery(query, writeLocations.get(pk));
    } catch (java.lang.Exception ex) {
      ex.printStackTrace();
      abort(eb_id);
    }
    return SHOPPING_ID;
  }

  public static Cart doCartWithinTxn(int eb_id,
                                     Map<primary_key, DMConnId> writeLocations,
                                     int SHOPPING_ID, Integer I_ID, Vector ids,
                                     Vector quantities) {
    DMConn conn = getConn(eb_id);

    Cart cart = null;
    try {
      if (I_ID != null) {
        addItemWithinTxn(eb_id, writeLocations, SHOPPING_ID, I_ID.intValue());
      }
      refreshCartWithinTxn(eb_id, writeLocations, SHOPPING_ID, ids, quantities);
      addRandomItemToCartIfNecessaryWithinTxn(eb_id, writeLocations,
                                              SHOPPING_ID);
      resetCartTimeWithinTxn(eb_id, writeLocations, SHOPPING_ID);
      cart = getCartWithinTxn(eb_id, SHOPPING_ID, 0.0);
    } catch (java.lang.Exception ex) {
      ex.printStackTrace();
      abort(eb_id);
    }
    return cart;
  }

  public static void
  resetCartTimeWithinTxn(int eb_id, Map<primary_key, DMConnId> writeLocations,
                         int SHOPPING_ID) {
    DMConn conn = getConn(eb_id);
    try {
      primary_key pk = DMUtil.constructShoppingCartPrimaryKey(SHOPPING_ID);
      String stmt = SQL.resetCartTime;
      String query = conn.constructQuery(stmt, String.valueOf(SHOPPING_ID));
      conn.executeWriteQuery(query, writeLocations.get(pk));
    } catch (java.lang.Exception ex) {
      ex.printStackTrace();
      abort(eb_id);
    }
  }

  public static void addRandomItemToCartIfNecessaryWithinTxn(
      int eb_id, Map<primary_key, DMConnId> writeLocations, int SHOPPING_ID) {
    DMConn conn = getConn(eb_id);

    int related_item = 0;
    try {
      String stmt = SQL.addRandomItemToCartIfNecessary;
      DMResultSet rs = conn.executeReadQuery(stmt, String.valueOf(SHOPPING_ID));
      if (rs.next()) {
        if (rs.getInt("COUNT(*)") == 0) {
          int randId = TPCW_Util.getRandomI_ID();
          related_item = getRelated1WithinTxn(eb_id, randId);
          addItemWithinTxn(eb_id, writeLocations, SHOPPING_ID, related_item);
        }
      }
      rs.close();
    } catch (java.lang.Exception ex) {
      ex.printStackTrace();
      System.out.println(
          "Adding entry to shopping cart failed: shopping id = " + SHOPPING_ID +
          " related_item = " + related_item);
      abort(eb_id);
    }
  }

  public static int getRelated1WithinTxn(int eb_id, int I_ID) {
    DMConn conn = getConn(eb_id);

    int related1 = -1;
    try {
      String stmt = SQL.getRelated1;
      DMResultSet rs = conn.executeReadQuery(stmt, String.valueOf(I_ID));
      if (rs.next()) {
        related1 = rs.getInt("i_related1");
      }
      rs.close();
    } catch (java.lang.Exception ex) {
      ex.printStackTrace();
      abort(eb_id);
    }
    return related1;
  }

  public static void
  refreshCartWithinTxn(int eb_id, Map<primary_key, DMConnId> writeLocations,
                       int SHOPPING_ID, Vector ids, Vector quantities) {
    DMConn conn = getConn(eb_id);

    int i;
    try {
      for (i = 0; i < ids.size(); i++) {
        String I_IDstr = (String)ids.elementAt(i);
        String QTYstr = (String)quantities.elementAt(i);
        int I_ID = Integer.parseInt(I_IDstr);
        int QTY = Integer.parseInt(QTYstr);

        String query = null;
        primary_key pk =
            DMUtil.constructShoppingCartLinePrimaryKey(SHOPPING_ID);

        if (QTY == 0) {
          String stmt = SQL.refreshCart_remove;
          query = conn.constructQuery(stmt, String.valueOf(SHOPPING_ID),
                                      String.valueOf(I_ID));
        } else {
          String stmt = SQL.refreshCart_update;
          query = conn.constructQuery(stmt, String.valueOf(QTY),
                                      String.valueOf(SHOPPING_ID),
                                      String.valueOf(I_ID));
        }

        conn.executeWriteQuery(query, writeLocations.get(pk));
      }
    } catch (java.lang.Exception ex) {
      ex.printStackTrace();
      abort(eb_id);
    }
  }

  public static void addItemWithinTxn(int eb_id,
                                      Map<primary_key, DMConnId> writeLocations,
                                      int SHOPPING_ID, int I_ID) {
    DMConn conn = getConn(eb_id);
    try {
      String addItemStmt = SQL.addItem;
      DMResultSet rs = conn.executeReadQuery(
          addItemStmt, String.valueOf(SHOPPING_ID), String.valueOf(I_ID));
      String query = null;
      primary_key pk = DMUtil.constructShoppingCartLinePrimaryKey(SHOPPING_ID);

      if (rs.next()) {
        int currqty = rs.getInt("scl_qty");
        currqty += 1;
        query = conn.constructQuery(SQL.addItem_update, String.valueOf(currqty),
                                    String.valueOf(SHOPPING_ID),
                                    String.valueOf(I_ID));
      } else {
        query =
            conn.constructQuery(SQL.addItem_put, String.valueOf(SHOPPING_ID),
                                "1", String.valueOf(I_ID));
      }
      rs.close();
      conn.executeWriteQuery(query, writeLocations.get(pk));
    } catch (java.lang.Exception ex) {
      ex.printStackTrace();
      abort(eb_id);
    }
  }

  public static Map<primary_key, DMConnId> beginAdminResponse(int eb_id,
                                                              int i_id) {
    List<primary_key> keys = new ArrayList<primary_key>();
    keys.add(DMUtil.constructItemPrimaryKey(i_id));

    return begin(eb_id, keys);
  }

  public static void
  adminUpdateWithinTxn(int eb_id, Map<primary_key, DMConnId> writeLocations,
                       int i_id, double cost, String image, String thumbnail) {
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
      rs.close();

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

  public static void getRelatedWithinTxn(int eb_id, int i_id, Vector i_id_vec,
                                         Vector i_thumbnail_vec) {
    DMConn conn = getConn(eb_id);

    try {
      String stmt = SQL.getRelated;
      DMResultSet rs = conn.executeReadQuery(stmt, String.valueOf(i_id));
      i_id_vec.removeAllElements();
      i_thumbnail_vec.removeAllElements();

      while (rs.next()) {
        i_id_vec.addElement(rs.getInt("i_id"));
        i_thumbnail_vec.addElement(rs.getString("i_thumbnail"));
      }
      rs.close();
    } catch (SQLException e) {
      e.printStackTrace();
      abort(eb_id);
    }
  }

  public static Vector getBestSellersWithinTxn(int eb_id, String subject) {
    Vector vec = new Vector();
    try {
      DMConn conn = getConn(eb_id);
      DMResultSet rs = conn.executeReadQuery(SQL.getBestSellers, "'" + subject + "'");

      while (rs.next()) {
        vec.addElement(new ShortBook(rs));
      }
      rs.close();
    } catch (SQLException e) {
      e.printStackTrace();
      abort(eb_id);
    }
    return vec;
  }

  public static String GetUserName(int eb_id, int cid) {
    DMConn conn = getConn(eb_id);
    String uname = null;
    try {
      begin(eb_id);

      String stmt = SQL.getUserName;
      DMResultSet rs = conn.executeReadQuery(stmt, String.valueOf(cid));
      if (rs.next()) {
        uname = rs.getString("c_uname");
      }
      rs.close();

      commit(eb_id);
    } catch (SQLException e) {
      e.printStackTrace();
      abort(eb_id);
    }
    return uname;
  }

  public static Vector doSubjectSearchWithinTxn(int eb_id, String searchKey) {
    String stmt = SQL.doSubjectSearch;
    return doBookAuthorSearchWithinTxn(eb_id, stmt, searchKey);
  }

  public static Vector doTitleSearchWithinTxn(int eb_id, String searchKey) {
    String stmt = SQL.doTitleSearch;
    return doBookAuthorSearchWithinTxn(eb_id, stmt, searchKey);
  }

  public static Vector doAuthorSearchWithinTxn(int eb_id, String searchKey) {
    String stmt = SQL.doAuthorSearch;
    return doBookAuthorSearchWithinTxn(eb_id, stmt, searchKey);
  }

  public static Vector doBookAuthorSearchWithinTxn(int eb_id, String stmt,
                                                   String searchKey) {
    DMConn conn = getConn(eb_id);

    Vector vec = new Vector();
    try {
      DMResultSet rs = conn.executeReadQuery(stmt, "'" + searchKey + "'");

      while (rs.next()) {
        Book b = new Book(rs);
        vec.addElement(b);
      }
      rs.close();
    } catch (SQLException e) {
      e.printStackTrace();
      abort(eb_id);
    }
    return vec;
  }

  public static String[] getNameWithinTxn(int eb_id, int cid) {
    DMConn conn = getConn(eb_id);

    String name[] = new String[2];
    try {
      String stmt = SQL.getName;
      DMResultSet rs = conn.executeReadQuery(stmt, String.valueOf(cid));
      if (rs.next()) {
        name[0] = rs.getString("c_fname");
        name[1] = rs.getString("c_lname");
      }
      rs.close();
    } catch (SQLException e) {
      e.printStackTrace();
      abort(eb_id);
    }
    return name;
  }

  public static Vector getNewProductsWithinTxn(int eb_id, String subject) {
    DMConn conn = getConn(eb_id);
    Vector vec = new Vector();
    try {
      String stmt = SQL.getNewProducts;
      DMResultSet rs = conn.executeReadQuery(stmt, "'" + subject + "'");
      while (rs.next()) {
        ShortBook b = new ShortBook(rs);
        vec.addElement(b);
      }
      rs.close();
    } catch (SQLException e) {
      e.printStackTrace();
      abort(eb_id);
    }
    return vec;
  }

  public static String GetPassword(int eb_id, String cUname) {
    DMConn conn = getConn(eb_id);
    if (cUname.equals("")) {
      return "";
    }
    String passwd = null;
    try {
      String stmt = SQL.getPassword;
      DMResultSet rs = conn.executeReadQuery(stmt, "'" + cUname + "'");
      if (rs.next()) {
        passwd = rs.getString("c_passwd");
      }
      rs.close();
    } catch (SQLException e) {
      e.printStackTrace();
      abort(eb_id);
    }
    return passwd;
  }

  public static Order GetMostRecentOrder(int eb_id, String cUname,
                                         Vector order_lines) {
    DMConn conn = getConn(eb_id);

    try {
      order_lines.removeAllElements();
      int order_id;
      Order order;

      String stmt = SQL.getMostRecentOrder_id;
      DMResultSet orderIdRS = conn.executeReadQuery(stmt, "'" + cUname + "'");
      if (orderIdRS.next()) {
        order_id = orderIdRS.getInt("o_id");
        orderIdRS.close();
      } else {
        orderIdRS.close();
        return null;
      }

      stmt = SQL.getMostRecentOrder_order;
      DMResultSet recentOrderRS =
          conn.executeReadQuery(stmt, String.valueOf(order_id));
      if (recentOrderRS.next()) {
        order = new Order(recentOrderRS);
        recentOrderRS.close();
      } else {
        recentOrderRS.close();
        return null;
      }

      stmt = SQL.getMostRecentOrder_lines;
      DMResultSet orderLinesRS =
          conn.executeReadQuery(stmt, String.valueOf(order_id));
      while (orderLinesRS.next()) {
        order_lines.addElement(new OrderLine(orderLinesRS));
      }
      orderLinesRS.close();
      return order;
    } catch (SQLException e) {
      e.printStackTrace();
      abort(eb_id);
    }
    return null;
  }

  public static Customer beginBuyRequestWithCustomer(int eb_id, String uname) {
    Customer cust = null;
    try {
      // FIRST do look up to find the cust id so you can modify it
      begin(eb_id);
      cust = getCustomerWithinTxn(eb_id, uname);
      commit(eb_id);

      List<primary_key> keys = new ArrayList<primary_key>();
      keys.add(DMUtil.constructCustomerPrimaryKey(cust.c_id));

      Map<primary_key, DMConnId> writeLocations = begin(eb_id, keys);
      refreshSessionWithinTxn(eb_id, writeLocations, cust.c_id);
    } catch (java.lang.Exception ex) {
      ex.printStackTrace();
      abort(eb_id);
    }
    return cust;
  }

  public static Customer beginBuyRequestNewCustomer(int eb_id, Customer cust) {
    DMConn conn = getConn(eb_id);
    try {
      // first figure out if we are going to need addresses
      begin(eb_id);
      int address_id = lookUpAddressWithinTxn(
          eb_id, cust.addr_street1, cust.addr_street2, cust.addr_city,
          cust.addr_state, cust.addr_zip, cust.co_name);
      boolean shouldInsertAddress = true;
      if (address_id != -1) {
        shouldInsertAddress = false;
      }
      commit(eb_id);

      int c_id = customerCounter.incrementAndGet();

      List<primary_key> keys = new ArrayList<primary_key>();
      keys.add(DMUtil.constructCustomerPrimaryKey(c_id));
      if (shouldInsertAddress) {
        address_id = addressCounter.incrementAndGet();
        keys.add(DMUtil.constructAddressPrimaryKey(address_id));
      }

      Map<primary_key, DMConnId> writeLocations = begin(eb_id, keys);

      cust.c_discount = (int)(java.lang.Math.random() * 51);
      cust.c_balance = 0.0;
      cust.c_ytd_pmt = 0.0;
      // FIXME - Use SQL CURRENT_TIME to do this
      cust.c_last_visit = new Date(System.currentTimeMillis());
      cust.c_since = new Date(System.currentTimeMillis());
      cust.c_login = new Date(System.currentTimeMillis());
      cust.c_expiration = new Date(System.currentTimeMillis() +
                                   7200000); // milliseconds in 2 hours
      cust.addr_street1 = cust.addr_street1.replaceAll("[^A-Za-z0-9]", "");
      cust.addr_street2 = cust.addr_street2.replaceAll("[^A-Za-z0-9]", "");
      cust.addr_city = cust.addr_city.replaceAll("[^A-Za-z0-9]", "");
      cust.co_name = cust.co_name.replaceAll("[^A-Za-z0-9]", "");

      cust.addr_id = address_id;

      if (shouldInsertAddress) {
        enterAddressWithinTxn(eb_id, writeLocations, address_id,
                              cust.addr_street1, cust.addr_street2,
                              cust.addr_city, cust.addr_state, cust.addr_zip,
                              cust.co_name);
      }
      createNewCustomerWithinTxn(eb_id, writeLocations, c_id, cust);

    } catch (java.lang.Exception ex) {
      ex.printStackTrace();
      abort(eb_id);
    }
    return cust;
  }

  public static void
  createNewCustomerWithinTxn(int eb_id,
                             Map<primary_key, DMConnId> writeLocations,
                             int c_id, Customer cust) {
    try {
      DMConn conn = getConn(eb_id);

      String maxIdStmt = SQL.createNewCustomer_maxId;
      DMResultSet rs = conn.executeReadQuery(maxIdStmt);
      if (rs.next()) {
        int max_c_id = rs.getInt("max");
      }
      rs.close();

      cust.c_id = c_id;
      cust.c_uname = TPCW_Util.DigSyl(cust.c_id, 0);
      cust.c_passwd = cust.c_uname.toLowerCase();
      cust.c_data = cust.c_data.replaceAll("[^A-Za-z0-9]", "");

      primary_key pk = DMUtil.constructCustomerPrimaryKey(c_id);
      String createNewCustomerStmt = SQL.createNewCustomer;
      SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
      String query = conn.constructQuery(
          createNewCustomerStmt, String.valueOf(cust.c_id),
          "'" + cust.c_uname + "'", "'" + cust.c_passwd + "'",
          "'" + cust.c_fname + "'", "'" + cust.c_lname + "'",
          String.valueOf(cust.addr_id), "'" + cust.c_phone + "'",
          "'" + cust.c_email + "'", "'" + sdf.format(cust.c_since) + "'",
          "'" + sdf.format(cust.c_last_visit) + "'",
          "'" + sdf.format(cust.c_login) + "'",
          "'" + sdf.format(cust.c_expiration) + "'",
          String.valueOf(cust.c_discount), String.valueOf(cust.c_balance),
          String.valueOf(cust.c_ytd_pmt),
          "'" + sdf.format(cust.c_birthdate) + "'", cust.c_data);

      conn.executeWriteQuery(query, writeLocations.get(pk));

    } catch (java.lang.Exception ex) {
      ex.printStackTrace();
      abort(eb_id);
    }
  }

  public static Customer getCustomerWithinTxn(int eb_id, String uname) {
    Customer cust = null;
    try {
      DMConn conn = getConn(eb_id);

      String stmt = SQL.getCustomer;
      DMResultSet rs = conn.executeReadQuery(stmt, "'" + uname + "'");
      if (rs.next()) {
        cust = new Customer(rs);
      }
	  rs.close();

    } catch (java.lang.Exception ex) {
      ex.printStackTrace();
      abort(eb_id);
    }
    return cust;
  }

  public static void
  refreshSessionWithinTxn(int eb_id, Map<primary_key, DMConnId> writeLocations,
                          int C_ID) {
    try {
      DMConn conn = getConn(eb_id);

      primary_key pk = DMUtil.constructCustomerPrimaryKey(C_ID);

      String stmt = SQL.refreshSession;
      String query = conn.constructQuery(stmt, String.valueOf(C_ID));

      conn.executeWriteQuery(query, writeLocations.get(pk));
    } catch (java.lang.Exception ex) {
      ex.printStackTrace();
      abort(eb_id);
    }
  }

  public static BuyConfirmResult
  doBuyConfirm(int eb_id, int shopping_id, int customer_id, String cc_type,
               long cc_number, String cc_name, Date cc_expiry, String shipping,
               String street_1, String street_2, String city, String state,
               String zip, String country) {

    BuyConfirmResult result = new BuyConfirmResult();
    try {
      // FIRST do look ups, so you know what to expect
      begin(eb_id);
      double c_discount = getCDiscountWithinTxn(eb_id, customer_id);
      result.cart = getCartWithinTxn(eb_id, shopping_id, c_discount);
      int address_id = lookUpAddressWithinTxn(eb_id, street_1, street_2, city,
                                              state, zip, country);
      boolean shouldInsertAddress = true;
      if (address_id != -1) {
        shouldInsertAddress = false;
      }
      commit(eb_id);

      int order_id = orderCounter.incrementAndGet();
      List<primary_key> keys = new ArrayList<primary_key>();
      keys.add(DMUtil.constructOrderPrimaryKey(order_id));
      keys.add(DMUtil.constructCCXactsPrimaryKey(order_id));
      keys.add(DMUtil.constructShoppingCartLinePrimaryKey(shopping_id));

      for (int ol_id = 0; ol_id < result.cart.lines.size(); ol_id++) {
        keys.add(DMUtil.constructOrderLinePrimaryKey(order_id, ol_id));
        CartLine cart_line = (CartLine)result.cart.lines.get(ol_id);
        keys.add(DMUtil.constructItemPrimaryKey(cart_line.scl_i_id));
      }

      if (shouldInsertAddress) {
        address_id = addressCounter.incrementAndGet();
        keys.add(DMUtil.constructAddressPrimaryKey(address_id));
      }

      Map<primary_key, DMConnId> writeLocations = begin(eb_id, keys);
      // now do the actual work
      int ship_addr_id = address_id;
      if (shouldInsertAddress) {
        enterAddressWithinTxn(eb_id, writeLocations, address_id, street_1,
                              street_2, city, state, zip, country);
      }

      result.order_id =
          enterOrderWithinTxn(eb_id, writeLocations, order_id, customer_id,
                              result.cart, ship_addr_id, shipping, c_discount);
      enterCCXactWithinTxn(eb_id, writeLocations, result.order_id, cc_type,
                           cc_number, cc_name, cc_expiry, result.cart.SC_TOTAL,
                           ship_addr_id);
      clearCartWithinTxn(eb_id, writeLocations, shopping_id);
    } catch (java.lang.Exception ex) {
      ex.printStackTrace();
      abort(eb_id);
    }
    return result;
  }

  public static BuyConfirmResult doBuyConfirm(int eb_id, int shopping_id,
                                              int customer_id, String cc_type,
                                              long cc_number, String cc_name,
                                              Date cc_expiry, String shipping) {
    BuyConfirmResult result = new BuyConfirmResult();
    try {
      // FIRST do look ups, so you know what to expect
      begin(eb_id);
      double c_discount = getCDiscountWithinTxn(eb_id, customer_id);
      result.cart = getCartWithinTxn(eb_id, shopping_id, c_discount);
      commit(eb_id);

      int order_id = orderCounter.incrementAndGet();
      List<primary_key> keys = new ArrayList<primary_key>();
      keys.add(DMUtil.constructOrderPrimaryKey(order_id));
      keys.add(DMUtil.constructCCXactsPrimaryKey(order_id));
      keys.add(DMUtil.constructShoppingCartLinePrimaryKey(shopping_id));

      for (int ol_id = 0; ol_id < result.cart.lines.size(); ol_id++) {
        keys.add(DMUtil.constructOrderLinePrimaryKey(order_id, ol_id));
        CartLine cart_line = (CartLine)result.cart.lines.get(ol_id);
        keys.add(DMUtil.constructItemPrimaryKey(cart_line.scl_i_id));
      }

      Map<primary_key, DMConnId> writeLocations = begin(eb_id, keys);
      // now do the actual work
      int ship_addr_id = getCAddrWithinTxn(eb_id, customer_id);
      result.order_id =
          enterOrderWithinTxn(eb_id, writeLocations, order_id, customer_id,
                              result.cart, ship_addr_id, shipping, c_discount);
      enterCCXactWithinTxn(eb_id, writeLocations, result.order_id, cc_type,
                           cc_number, cc_name, cc_expiry, result.cart.SC_TOTAL,
                           ship_addr_id);
      clearCartWithinTxn(eb_id, writeLocations, shopping_id);

      commit(eb_id);
    } catch (java.lang.Exception ex) {
      ex.printStackTrace();
      abort(eb_id);
    }
    return result;
  }

  public static int
  enterAddressWithinTxn(int eb_id, Map<primary_key, DMConnId> writeLocations,
                        int addr_id, String street1, String street2,
                        String city, String state, String zip, String country) {
    DMConn conn = getConn(eb_id);
    try {
      int addr_co_id = getCountryIdWithinTxn(eb_id, country);

      street1 = street1.replaceAll("[^A-Za-z0-9]", "");
      street2 = street2.replaceAll("[^A-Za-z0-9]", "");
      city = city.replaceAll("[^A-Za-z0-9]", "");

      // Miss on addr table
      String getMaxAddrIdStmt = SQL.enterAddress_maxId;
      DMResultSet rs = conn.executeReadQuery(getMaxAddrIdStmt);
      if (rs.next()) {
        int addr_id_max = rs.getInt("max") + 1;
      }
      rs.close();

      primary_key pk = DMUtil.constructAddressPrimaryKey(addr_id);
      String insertAddrStmt = SQL.enterAddress_insert;
      String query = conn.constructQuery(
          insertAddrStmt, String.valueOf(addr_id), "'" + street1 + "'",
          "'" + street2 + "'", "'" + city + "'", "'" + state + "'",
          "'" + zip + "'", String.valueOf(addr_co_id));
      conn.executeWriteQuery(query, writeLocations.get(pk));
    } catch (java.lang.Exception ex) {
      ex.printStackTrace();
      abort(eb_id);
    }
    return addr_id;
  }

  public static int lookUpAddressWithinTxn(int eb_id, String street1,
                                           String street2, String city,
                                           String state, String zip,
                                           String country) {
    DMConn conn = getConn(eb_id);

    int addr_id = -1;
    try {
      int addr_co_id = getCountryIdWithinTxn(eb_id, country);

      street1 = street1.replaceAll("[^A-Za-z0-9]", "");
      street2 = street2.replaceAll("[^A-Za-z0-9]", "");
      city = city.replaceAll("[^A-Za-z0-9]", "");

      String stmt = SQL.enterAddress_match;
      DMResultSet rs = conn.executeReadQuery(
          stmt, "'" + street1 + "'", "'" + street2 + "'", "'" + city + "'",
          "'" + state + "'", "'" + zip + "'", String.valueOf(addr_co_id));
      // Hit on addr table
      if (rs.next()) {
        addr_id = rs.getInt("addr_id");
      }
      rs.close();
    } catch (java.lang.Exception ex) {
      ex.printStackTrace();
      abort(eb_id);
    }
    return addr_id;
  }

  public static int getCountryIdWithinTxn(int eb_id, String country) {
    DMConn conn = getConn(eb_id);

    int addr_co_id = 0;
    try {
      String stmt = SQL.enterAddress_id;
      DMResultSet rs = conn.executeReadQuery(stmt, "'" + country + "'");
      if (rs.next()) {
        addr_co_id = rs.getInt("co_id");
      }
      rs.close();
    } catch (java.lang.Exception ex) {
      ex.printStackTrace();
      abort(eb_id);
    }
    return addr_co_id;
  }

  public static void
  clearCartWithinTxn(int eb_id, Map<primary_key, DMConnId> writeLocations,
                     int shopping_id) {
    DMConn conn = getConn(eb_id);
    try {
      primary_key pk = DMUtil.constructShoppingCartLinePrimaryKey(shopping_id);
      String stmt = SQL.clearCart;
      String query = conn.constructQuery(stmt, String.valueOf(shopping_id));
      conn.executeWriteQuery(query, writeLocations.get(pk));
    } catch (java.lang.Exception ex) {
      ex.printStackTrace();
      abort(eb_id);
    }
  }

  public static void
  enterCCXactWithinTxn(int eb_id, Map<primary_key, DMConnId> writeLocations,
                       int o_id, String cc_type, long cc_number, String cc_name,
                       Date cc_expiry, double total, int ship_addr_id) {
    DMConn conn = getConn(eb_id);

    SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");

    if (cc_type.length() > 10)
      cc_type = cc_type.substring(0, 10);
    if (cc_name.length() > 30)
      cc_name = cc_name.substring(0, 30);
    cc_name = cc_name.replaceAll("[^A-Za-z0-9]", "");
    try {
      primary_key pk = DMUtil.constructCCXactsPrimaryKey(o_id);
      String stmt = SQL.enterCCXact;
      String query = conn.constructQuery(
          stmt, String.valueOf(o_id), "'" + cc_type + "'",
          String.valueOf(cc_number), "'" + cc_name + "'",
          "'" + sdf.format(cc_expiry) + "'", String.valueOf(total),
          String.valueOf(ship_addr_id));
      conn.executeWriteQuery(query, writeLocations.get(pk));

    } catch (java.lang.Exception ex) {
      ex.printStackTrace();
      abort(eb_id);
    }
  }

  public static int
  enterOrderWithinTxn(int eb_id, Map<primary_key, DMConnId> writeLocations,
                      int o_id, int customer_id, Cart cart, int ship_addr_id,
                      String shipping, double c_discount) {
    DMConn conn = getConn(eb_id);
    int o_id_max = 0;
    // The code performs another read to get the address id as the
    // ship_addr_id,
    // change this if this is not correct
    int bill_addr_id = ship_addr_id;
    try {
      String getMaxIdStmt = SQL.enterOrder_maxId;
      DMResultSet rs = conn.executeReadQuery(getMaxIdStmt);
      if (rs.next()) {
        o_id_max = rs.getInt("max") + 1;
      }
      rs.close();

      primary_key pk = DMUtil.constructOrderPrimaryKey(o_id);

      String enterOrderStmt = SQL.enterOrder_insert;
      String query = conn.constructQuery(
          enterOrderStmt, String.valueOf(o_id), String.valueOf(customer_id),
          String.valueOf(cart.SC_SUB_TOTAL), String.valueOf(cart.SC_TOTAL),
          "'" + shipping + "'", String.valueOf(TPCW_Util.getRandom(7)),
          String.valueOf(bill_addr_id), String.valueOf(ship_addr_id));
      conn.executeWriteQuery(query, writeLocations.get(pk));

    } catch (java.lang.Exception ex) {
      ex.printStackTrace();
      abort(eb_id);
    }

    Enumeration e = cart.lines.elements();
    int counter = 0;
    while (e.hasMoreElements()) {
      // - Creates one or more 'order_line' rows.
      CartLine cart_line = (CartLine)e.nextElement();
      addOrderLineWithinTxn(eb_id, writeLocations, counter, o_id,
                            cart_line.scl_i_id, cart_line.scl_qty, c_discount,
                            TPCW_Util.getRandomString(20, 100));
      counter++;

      // - Adjusts the stock for each item ordered
      int stock = getStockWithinTxn(eb_id, cart_line.scl_i_id);
      if ((stock - cart_line.scl_qty) < 10) {
        setStockWithinTxn(eb_id, writeLocations, cart_line.scl_i_id,
                          stock - cart_line.scl_qty + 21);
      } else {
        setStockWithinTxn(eb_id, writeLocations, cart_line.scl_i_id,
                          stock - cart_line.scl_qty);
      }
    }
    return o_id;
  }

  public static int getStockWithinTxn(int eb_id, int i_id) {
    DMConn conn = getConn(eb_id);
    int stock = 0;
    try {
      String stmt = SQL.getStock;
      DMResultSet rs = conn.executeReadQuery(stmt, String.valueOf(i_id));
      if (rs.next()) {
        stock = rs.getInt("i_stock");
      }
      rs.close();
    } catch (java.lang.Exception ex) {
      ex.printStackTrace();
      abort(eb_id);
    }
    return stock;
  }

  public static void
  setStockWithinTxn(int eb_id, Map<primary_key, DMConnId> writeLocations,
                    int i_id, int new_stock) {
    DMConn conn = getConn(eb_id);

    try {
      primary_key pk = DMUtil.constructItemPrimaryKey(i_id);
      String stmt = SQL.setStock;
      String query = conn.constructQuery(stmt, String.valueOf(i_id),
                                         String.valueOf(new_stock));
      conn.executeWriteQuery(query, writeLocations.get(pk));
    } catch (java.lang.Exception ex) {
      ex.printStackTrace();
      abort(eb_id);
    }
  }

  public static void
  addOrderLineWithinTxn(int eb_id, Map<primary_key, DMConnId> writeLocations,
                        int ol_id, int ol_o_id, int ol_i_id, int ol_qty,
                        double ol_discount, String ol_comment) {
    DMConn conn = getConn(eb_id);

    int success = 0;
    try {
      primary_key pk = DMUtil.constructOrderLinePrimaryKey(ol_o_id, ol_id);
      String stmt = SQL.addOrderLine;
      String query = conn.constructQuery(
          stmt, String.valueOf(ol_id), String.valueOf(ol_o_id),
          String.valueOf(ol_i_id), String.valueOf(ol_qty),
          String.valueOf(ol_discount), "'" + ol_comment + "'");
      conn.executeWriteQuery(query, writeLocations.get(pk));
    } catch (java.lang.Exception ex) {
      ex.printStackTrace();
      abort(eb_id);
    }
  }

  public static double getCDiscountWithinTxn(int eb_id, int c_id) {
    DMConn conn = getConn(eb_id);
    double c_discount = 0;
    try {
      String stmt = SQL.getCDiscount;
      DMResultSet rs = conn.executeReadQuery(stmt, String.valueOf(c_id));
      if (rs.next()) {
        c_discount = rs.getDouble("c_discount");
      }
      rs.close();
    } catch (java.lang.Exception ex) {
      ex.printStackTrace();
      abort(eb_id);
    }
    return c_discount;
  }

  public static int getCAddrWithinTxn(int eb_id, int c_id) {
    DMConn conn = getConn(eb_id);

    int c_addr_id = 0;
    try {
      String stmt = SQL.getCAddr;
      DMResultSet rs = conn.executeReadQuery(stmt, String.valueOf(c_id));
      if (rs.next()) {
        c_addr_id = rs.getInt("c_addr_id");
      }
      rs.close();
    } catch (java.lang.Exception ex) {
      ex.printStackTrace();
      abort(eb_id);
    }
    return c_addr_id;
  }

  public static Cart getCartWithinTxn(int eb_id, int SHOPPING_ID,
                                      double c_discount) {
    DMConn conn = getConn(eb_id);

    Cart myCart = null;
    try {
      String stmt = SQL.getCart;
      DMResultSet rs = conn.executeReadQuery(stmt, String.valueOf(SHOPPING_ID));
      myCart = new Cart(rs, c_discount);
    } catch (java.lang.Exception ex) {
      ex.printStackTrace();
      abort(eb_id);
    }
    return myCart;
  }

  public static void initialize() {
    boolean shouldInitialize = initializationStage.compareAndSet(0, 1);
    if (!shouldInitialize) {
      // initialization phase has completed
      int initValue = initializationStage.get();
      assertD(
          initValue == 2,
          "somehow lost race but got initialization stage of not 2, it is " +
              initValue);
      // wait until things are good to release
      log.info("stage 2 reached");
      return;
    }

    try {
      log.info("Getting maxes, initializationStage is:{}",
               initializationStage.get());
      addressCounter.set(getMaxFromTable("address", "addr_id"));
      log.info("Getting max orders");
      orderCounter.set(getMaxFromTable("orders", "o_id"));
      log.info("Getting max customer");
      customerCounter.set(getMaxFromTable("customer", "c_id"));
      log.info("Getting max shopping cart");
      shoppingCartCounter.set(getMaxFromTable("shopping_cart", "sc_id"));
      /*
      */
      // TBD
      // shoppingCartLineCounter.set(getMaxFromTable("shopping_cart_line",
      // "addr_id"));

      log.info("Notifying initialization done");
      assertD(initializationStage.compareAndSet(1, 2),
              "Failed to compare and set");
      log.info("initialization stage set to: {}", initializationStage.get());
    } catch (java.lang.Exception ex) {
      ex.printStackTrace();
      assertD(true, "Hit exception while doing stuff");
    }
    return;
  }

  private static int getMaxFromTable(String tableName, String column) {
    int max = 0;
    int eb_id = 0;
    try {
      log.info("Calling get max from table: {}", tableName);
      DMConn conn = getConn(eb_id);
	  // don't screw myself if this is empty, because apparently pg returns a
	  // null record
      String query = "SELECT coalesce(max(" + column + "), 0) FROM " + tableName;
      log.info("Execute single read query:{}", query);
      DMResultSet rs = conn.executeSingleReadQuery(query);

      if (rs.next()) {
        max = rs.getInt("coalesce");
      }
      rs.close();
    } catch (SQLException e) {
      System.out.println("Unable to get max from table:" + tableName +
                         ", col:" + column);
      e.printStackTrace();
      abort(eb_id);
      // this is mega bad, we are pretty screwed if this happens
      System.exit(1);
    }
	log.info("Max for table:{} is: {}", tableName, max);
    return max;
  }

  public static void begin(int eb_id) {
    // don't care let it go away;
    Map<primary_key, DMConnId> writeLocations =
        begin(eb_id, new ArrayList<primary_key>());
  }

  public static Map<primary_key, DMConnId> begin(int eb_id,
                                                 List<primary_key> keys) {
    DMConn conn = getConn(eb_id);
    Map<primary_key, DMConnId> writeLocations = null;
    try {
      writeLocations = conn.begin(keys);
    } catch (SQLException e) {
      e.printStackTrace();
      abort(eb_id);
    }
    return writeLocations;
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

  public static void assertD(boolean cond, String logMsg) {
	if (cond) {
		return;
	}
    System.out.println("!!!!!!! ASSERT TRIGGERED!!!!!\n, here is a stack trace:");
    Thread.dumpStack();
    System.out.println("!!!!!!! Message is:" + logMsg);
    System.out.println("!!!!!!! NOW DYING !!!");
    System.exit(1);
  }
}
