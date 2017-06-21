package servlets;

import java.util.Arrays;
import java.util.ArrayList;
import java.util.List;
import dm.DMClient;
import dm.DMConn;

import com.dynamic.mastering.*;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DMUtil {
  private static Logger log = LoggerFactory.getLogger(DMUtil.class);

  public static DMConn makeDMConnection(long terminalID) {
    String connStr = "tem09:9091";
	log.info("Making client to:{} as id: {}", connStr, terminalID);
    DMClient client = DMConn.constructDMClient(connStr, (int)terminalID);
	log.info("Making conn to:{} as id: {}", connStr, terminalID);
    return new DMConn(client);
  }

  public static primary_key constructAddressPrimaryKey(int addr_id) {

	long row_id = intsIntoLong(0, addr_id);
    return new primary_key(DMConstants.ADDRESS_TABLE_ID, row_id);
  }

  public static primary_key constructAuthorPrimaryKey(int auth_id) {
	long row_id = intsIntoLong(0, auth_id);
    return new primary_key(DMConstants.AUTHOR_TABLE_ID, row_id);
  }

  public static primary_key constructCustomerPrimaryKey(int cust_id) {
	long row_id = intsIntoLong(0, cust_id);
    return new primary_key(DMConstants.CUSTOMER_TABLE_ID, row_id);
  }

  public static primary_key constructCountryPrimaryKey(int country_id) {
	long row_id = intsIntoLong(0, country_id);
    return new primary_key(DMConstants.COUNTRY_TABLE_ID, row_id);
  }

  public static primary_key constructItemPrimaryKey(int item_id) {
	long row_id = intsIntoLong(0, item_id);
    return new primary_key(DMConstants.ITEM_TABLE_ID, row_id);
  }

  public static primary_key constructOrderPrimaryKey(int order_id) {
	long row_id = intsIntoLong(0, order_id);
    return new primary_key(DMConstants.ORDER_TABLE_ID, row_id);
  }

  public static primary_key constructCCXactsPrimaryKey(int order_id) {
	long row_id = intsIntoLong(0, order_id);
    return new primary_key(DMConstants.CC_XACTS_TABLE_ID, row_id);
  }

  public static primary_key constructOrderLinePrimaryKey(int order_id,
                                                         int order_line_id) {

	long row_id = intsIntoLong(order_line_id, order_id);
    return new primary_key(DMConstants.ORDER_LINE_TABLE_ID, row_id);
  }

  public static primary_key
  constructShoppingCartPrimaryKey(int shopping_cart_id) {
	long row_id = intsIntoLong(0, shopping_cart_id);
    return new primary_key(DMConstants.SHOPPING_CART_TABLE_ID,
                           row_id);
  }

  // TODO decide if this should take an item id as well for finer grain control
  public static primary_key
  constructShoppingCartLinePrimaryKey(int shopping_cart_id) {
	long row_id = intsIntoLong(0, shopping_cart_id);
    return new primary_key(DMConstants.SHOPPING_CART_LINE_TABLE_ID,
                           row_id);
  }

  public static DMKeyToItemMeaning generateMeaningFromKey(primary_key pk) {
    int tableId = pk.table_id;
    long rowId = pk.row_id;
    int[] intsInRowId = longToInts(rowId);
    int hi = intsInRowId[0];
    int lo = intsInRowId[1];

    switch (tableId) {
    case 1:
      return singleItemMeaning("address", "addr_id", lo);
	case 2:
	  return singleItemMeaning("author", "a_id", lo);
    case 3:
	  return singleItemMeaning("country", "co_id", lo);
	case 4:
	  return singleItemMeaning("customer", "c_id", lo);
    case 5:
	  return singleItemMeaning("item", "i_id", lo);
	case 6:
	  return singleItemMeaning("orders", "o_id", lo);
    case 7:
	  return singleItemMeaning("cc_xacts", "cx_o_id", lo);
	case 8:
	  return generateMeaningFromOrderLineKey(hi, lo);
    case 9:
	  return singleItemMeaning("shopping_cart", "sc_id", lo);
	case 10:
	  return generateMeaningFromShoppingCartLineKey(hi, lo);


    }
    return null;
  }

  public static DMKeyToItemMeaning generateMeaningFromShoppingCartLineKey(int hi, int lo) {
	  return singleItemMeaning("shopping_cart_line", "scl_sc_id", lo);
  }
  public static DMKeyToItemMeaning generateMeaningFromOrderLineKey(int hi,
                                                                   int lo) {
    List<String> columns = Arrays.asList("ol_id", "ol_o_id");
    List<String> identifiers =
        Arrays.asList(String.valueOf(hi), String.valueOf(lo));
    return new DMKeyToItemMeaning("order_line", columns, identifiers);
  }

  public static DMKeyToItemMeaning singleItemMeaning(String table, String column, int id) {
    return new DMKeyToItemMeaning(table, Arrays.asList(column),
                                  Arrays.asList(String.valueOf(id)));
  }

  // first is hi, second is lo
  // aka always returns the same parameters as the intsInto long argument
  public static int[] longToInts(long l) {
    int hi = (int)(l >> 32);
    int lo = (int)l;
    return new int[] {hi, lo};
  }


  // stop, are you changing parameter order, if so make sure update longToInts
  public static long intsIntoLong(int hi, int lo) {
	// https://stackoverflow.com/a/21592598
	return (((long) hi)) << 32 | (lo & 0xffffffffL);
  }

  public static String sanitize(String input) {
    // this is really annoying
    // keep @ and . for emails
    String output = input.replaceAll("[^A-Za-z0-9@\\.\\']", "");
	return output;
  }
}
