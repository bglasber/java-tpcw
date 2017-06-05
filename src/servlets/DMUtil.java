package servlets;

import dm.DMClient;
import dm.DMConn;

public class DMUtil {
  public static DMConn makeDMConnection(long terminalID) {
    String connStr = "tem09:9091";
    DMClient client = DMConn.constructDMClient(connStr, (int)terminalID);
    return new DMConn(client);
  }
}
