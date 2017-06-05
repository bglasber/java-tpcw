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

}
