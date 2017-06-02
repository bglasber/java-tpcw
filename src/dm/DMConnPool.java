package dm;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.thrift.TException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DMConnPool {
  private Logger log = LoggerFactory.getLogger(this.getClass());
  private ConcurrentHashMap<DMConnId, DMSiteManagerClient> smClients;
  private ConcurrentHashMap<DMConnId, DMSiteSelectorClient> ssClients;

  public DMConnPool() {
    smClients = new ConcurrentHashMap<DMConnId, DMSiteManagerClient>();
    ssClients = new ConcurrentHashMap<DMConnId, DMSiteSelectorClient>();
  }

  public DMSiteManagerClient getSiteManager(DMConnId connId) throws TException {
    DMSiteManagerClient client = smClients.get(connId);
    if (client == null) {
      DMSiteManagerClient newClient = new DMSiteManagerClient(connId);
      client = smClients.putIfAbsent(connId, newClient);
      if (client == null) {
        client = newClient;
      } else {
        // lost the race
        newClient.close();
      }
    }
    return client;
  }

  public DMSiteSelectorClient getSiteSelector(DMConnId connId)
      throws TException {
    DMSiteSelectorClient client = ssClients.get(connId);
    if (client == null) {
      DMSiteSelectorClient newClient = new DMSiteSelectorClient(connId);
      client = ssClients.putIfAbsent(connId, newClient);
      if (client == null) {
        client = newClient;
      } else {
        // lost the race
        newClient.close();
      }
    }
    return client;
  }

  public void close() {
    for (Map.Entry<DMConnId, DMSiteManagerClient> entry :
         smClients.entrySet()) {
      try {
        entry.getValue().close();
      } catch (TException x) {
        log.error("Error closing site manager connection:", x.getMessage());
      }
    }
    smClients.clear();

    for (Map.Entry<DMConnId, DMSiteSelectorClient> entry :
         ssClients.entrySet()) {
      try {
        entry.getValue().close();
      } catch (TException x) {
        log.error("Error closing site selector connection:", x.getMessage());
      }
    }

    ssClients.clear();
  }
}

