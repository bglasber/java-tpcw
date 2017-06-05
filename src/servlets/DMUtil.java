package servlets;

import dm.DMClient;
import dm.DMConn;

import com.dynamic.mastering.*;

public class DMUtil {
  public static DMConn makeDMConnection(long terminalID) {
    String connStr = "tem09:9091";
    DMClient client = DMConn.constructDMClient(connStr, (int)terminalID);
    return new DMConn(client);
  }

  public static primary_key constructAddressPrimaryKey(int addr_id) {
    return new primary_key(DMConstants.ADDRESS_TABLE_ID, addr_id);
  }

  public static primary_key constructAuthorPrimaryKey(int auth_id) {
    return new primary_key(DMConstants.AUTHOR_TABLE_ID, auth_id);
  }

  public static primary_key constructCustomerPrimaryKey(int cust_id) {
    return new primary_key(DMConstants.CUSTOMER_TABLE_ID, cust_id);
  }

  public static primary_key constructCountryPrimaryKey(int country_id) {
    return new primary_key(DMConstants.COUNTRY_TABLE_ID, country_id);
  }

  public static primary_key constructItemPrimaryKey(int item_id) {
    return new primary_key(DMConstants.ITEM_TABLE_ID, item_id);
  }


}
