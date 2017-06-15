package dm;

import java.io.IOException;
import java.lang.Math;
import java.nio.ByteBuffer;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.HashMap;
import java.util.concurrent.Callable;
import java.util.concurrent.Executors;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.Future;

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
import dm.future.*;

public class DMClient {
  private Logger log = LoggerFactory.getLogger(this.getClass());
  private ExecutorService threadPool;

  private DMSiteSelectorClient siteSelectorClient;
  private DMSiteManagerClient siteManagerClient;
  private DMConnPool connPool;
  private List<DMConnId> siteManagerIds;
  private List<Integer> client_session_version_vector;
  private int clientId;
  private long GLOBAL_START_TIME;
  private long GLOBAL_END_TIME;

  public DMClient(DMConnId connId, int clientId,
                  List<Integer> client_session_version_vector)
      throws TException {
	// TODO make this configurable
	threadPool = Executors.newFixedThreadPool(10);
    connPool = new DMConnPool();
    siteSelectorClient = new DMSiteSelectorClient(connId);
	siteManagerIds = new ArrayList<DMConnId>();
    siteManagerClient = null;
    this.client_session_version_vector = client_session_version_vector;
	this.clientId = clientId;
  }

  List<Integer> getClientSessionVersionVector() {
    return client_session_version_vector;
  }

  public Map<primary_key, DMConnId> beginTransaction(List<primary_key> write_set) {
	Map<primary_key, DMConnId> writeMap = new HashMap<primary_key, DMConnId>();
	siteManagerIds = new ArrayList<DMConnId>();
    try {
      long bStartTime = System.nanoTime();
	  GLOBAL_START_TIME = System.nanoTime();

      List<primary_key> dedupe_write_set =
          new ArrayList<primary_key>(new LinkedHashSet<primary_key>(write_set));
      log.info("begin with svv:{}", client_session_version_vector.toString());
      List<site_selector_begin_result> results = siteSelectorClient.beginTransaction(
          clientId, client_session_version_vector, dedupe_write_set);
	  if (results.isEmpty()) {
		  checkResponse(exec_status_type.MASTER_CHANGE_ERROR,
                        "error beginning transaction at the site selector",
                        "Unable to begin transaction");
		  return null;
	  }
	  boolean messedUp = false;
	  for (site_selector_begin_result result : results) {
		  if (!checkResponse(result.status,
							 "error beginning transaction at the site selector",
							 "Unable to begin transaction")) {
			messedUp = true;
		  }
		  DMConnId smConnId = new DMConnId(result.ip, result.port);
		  siteManagerClient = openSiteManagerClient(smConnId);
		  siteManagerIds.add(smConnId);
		  for (primary_key pk : result.items_at_site) {
			writeMap.put(pk, smConnId);
		  }
	  }
	  if (messedUp) {
		  return null;
	  }
      long bEndTime = System.nanoTime();
      log.info("begin took:{} ns", (bEndTime - bStartTime));

    } catch (TException x) {
      log.error("Error beginning transaction:{}", x.getMessage());
      log.error("Error", x);
	  return null;
    }
    return writeMap;
  }

  public boolean commitTransaction() {
	boolean decision = false;
	if (siteManagerIds.size() == 1) {
		decision = commitClientTransaction(siteManagerClient);
	} else {
		decision = twoPC();
	}
	if (decision) {
		long GLOBAL_END_TIME = System.nanoTime();
        log.info("{} client {} EndCommit: took: {} ns", new java.util.Date(), clientId, (GLOBAL_END_TIME - GLOBAL_START_TIME));
		siteManagerClient = null;
	}
	return decision;
  }

  private boolean twoPC() {
    try {
      boolean prepareOk = prepareForCommit();
      if (prepareOk) {
        return distributedCommit();
      } else {
		log.error("Reached a global abort decision for client:{}", clientId);
        return false;
      }
    } catch (InterruptedException ex) {
      log.error("Interruption on twoPC call:", ex.getMessage());
      log.error("trying to get stack trace", ex);
    } catch (ExecutionException ex) {
      log.error("Execution error on twoPC call:", ex.getMessage());
      log.error("trying to get stack trace", ex);
    }
    return false;
  }

  private boolean prepareForCommit() throws InterruptedException, ExecutionException {
    List<Future<commit_result>> futures = new ArrayList<Future<commit_result>>();
	log.info("Preparing for commit at: {}", siteManagerIds.toString());
    for (DMConnId connID : siteManagerIds) {
      Callable<commit_result> exec = new PrepareExecutor(getSiteManager(connID), clientId);
      Future<commit_result> f = threadPool.submit(exec);
      futures.add(f);
    }

    boolean error = true;

    for (Future<commit_result> f : futures) {
      commit_result res = f.get();
      if (!checkResponse(res.status, "Something went wrong with prepare",
                         "Unable to prepare transaction")) {
        error = false;
      }
	}
    return error;
  }

  private boolean distributedCommit()
      throws InterruptedException, ExecutionException {
    log.info("Beginning distributed commit");
    List<Future<commit_result>> futures =
        new ArrayList<Future<commit_result>>();
    for (DMConnId connID : siteManagerIds) {
      Callable<commit_result> exec =
          new CommitExecutor(getSiteManager(connID), clientId);
      Future<commit_result> f = threadPool.submit(exec);
      futures.add(f);
    }

    boolean error = true;
    for (Future<commit_result> f : futures) {
      commit_result res = f.get();
      if (!checkResponse(res.status, res.errMsg,
                         "Unable to commit transaction")) {
        error = false;
      } else {
        mergeSVV(res.session_version_vector);
        log.info("svv for client:{}, is: {}", clientId,
                 client_session_version_vector.toString());
      }
    }
    return error;
  }

  private boolean distributedAbort() throws InterruptedException, ExecutionException {
    List<Future<abort_result>> futures = new ArrayList<Future<abort_result>>();
    for (DMConnId connID : siteManagerIds) {
      Callable<abort_result> exec = new AbortExecutor(getSiteManager(connID), clientId);
      Future<abort_result> f = threadPool.submit(exec);
	  futures.add(f);
    }

	boolean error = true;

	for (Future<abort_result> f : futures) {
		abort_result res = f.get();
		if (!checkResponse(res.status, res.errMsg,
                       "Unable to abort transaction")) {
			error = false;
		}
	}
	return error;
  }

  private boolean commitClientTransaction(DMSiteManagerClient client) {
    CommitExecutor exec = new CommitExecutor(client, clientId);
    commit_result res = exec.commit();
    boolean success = true;
    if (!checkResponse(res.status, res.errMsg,
                       "Unable to commit transaction")) {
      success = false;
    } else {
      // update the svv
      client_session_version_vector = res.session_version_vector;
	  log.info("svv for client:{}, is: {}", clientId, client_session_version_vector.toString());
    }
    return success;
  }

  public boolean abortTransaction() {
    try {
	  boolean decision = false;
      if (siteManagerIds.size() == 1) {
        decision = abortClientTransaction(siteManagerClient);
      } else {
        decision = distributedAbort();
      }

      siteManagerClient = null;
	  GLOBAL_END_TIME = System.nanoTime();
      log.info("{} client {} EndAbort: took: {} ns", new java.util.Date(), clientId, (GLOBAL_END_TIME - GLOBAL_START_TIME));
      return decision;

    } catch (InterruptedException ex) {
      log.error("Interruption on abort call:", ex.getMessage());
      log.error("trying to get stack trace", ex);
    } catch (ExecutionException ex) {
      log.error("Execution error on abort call:", ex.getMessage());
      log.error("trying to get stack trace", ex);
    }

	GLOBAL_END_TIME = System.nanoTime();
	log.info("{} client {} EndAbort: took: {} ns", new java.util.Date(), clientId, (GLOBAL_END_TIME - GLOBAL_START_TIME));
	return false;
  }

  private boolean abortClientTransaction(DMSiteManagerClient client) {
    AbortExecutor exec = new AbortExecutor(client, clientId);
    abort_result res = exec.abort();
	boolean success = true;
    if (!checkResponse(res.status, res.errMsg, "Unable to abort transaction")) {
      success = false;
    }
    return success;
  }

  public boolean close() {
    try {
      log.error("Closing client for: {}", clientId);
	  threadPool.shutdown();
      siteSelectorClient.close();
      connPool.close();
    } catch (TException x) {
      log.error("Error closing connections:", x.getMessage());
      return false;
    }
    return true;
  }

  private query_result readQuery(String query, DMSiteManagerClient client) {
    query_result result = null;
    QueryExecutor exec = new QueryExecutor(client, clientId, true, query);
    result = exec.query();
    if (!checkResponse(result.status, result.errMsg,
                       "Unable to perform read query")) {
      return null;
    }

    return result;
  }

  public query_result readQuery(String query) {
	  return readQuery(query, siteManagerClient);
  }

  public query_result readQuery(String query, DMConnId clientId) {
	  return readQuery(query, getSiteManager(clientId));
  }

  public List<query_result> readQuery(Map<DMConnId, String> queries) {
    try {
		return distributedQuery(queries, true);
    } catch (InterruptedException ex) {
      log.error("Interruption on readQuery call:", ex.getMessage());
      log.error("trying to get stack trace", ex);
    } catch (ExecutionException ex) {
      log.error("Execution error on readQuery call:", ex.getMessage());
      log.error("trying to get stack trace", ex);
    }
    return new ArrayList<query_result>();
  }

  private List<query_result> distributedQuery(Map<DMConnId, String> queries, boolean isReadQuery)
      throws InterruptedException, ExecutionException {
    List<Future<query_result>> futures = new ArrayList<Future<query_result>>();
    for (Map.Entry<DMConnId, String>  queryEntry : queries.entrySet()) {
      Callable<query_result> exec =
          new QueryExecutor(getSiteManager(queryEntry.getKey()), clientId,
                            isReadQuery, queryEntry.getValue());
      Future<query_result> f = threadPool.submit(exec);
	  futures.add(f);
    }

	List<query_result> query_results = new ArrayList<query_result>();
	for (Future<query_result> f : futures) {
		query_result res = f.get();
		if (!checkResponse(res.status, res.errMsg,
                       "Unable to issue query")) {
			query_results.add(res);
		}
	}
	return query_results;
  }

  private query_result writeQuery(String query, DMSiteManagerClient client) {
    query_result result = null;
    QueryExecutor exec = new QueryExecutor(client, clientId, false, query);
    result = exec.query();
    if (!checkResponse(result.status, result.errMsg,
                       "Unable to perform write query")) {
      return null;
    }
    return result;
  }

  public query_result writeQuery(String query) {
	  return writeQuery(query, siteManagerClient);
  }

  public query_result writeQuery(String query, DMConnId clientId) {
	  return writeQuery(query, getSiteManager(clientId));
  }

  public List<query_result> writeQuery(Map<DMConnId, String> queries) {
    try {
		return distributedQuery(queries, false);
    } catch (InterruptedException ex) {
      log.error("Interruption on writeQuery call:", ex.getMessage());
      log.error("trying to get stack trace", ex);
    } catch (ExecutionException ex) {
      log.error("Execution error on readQuery call:", ex.getMessage());
      log.error("trying to get stack trace", ex);
    }
    return new ArrayList<query_result>();
  }

  public boolean storedProcedure(String name, Map<DMConnId, ByteBuffer> args) {
    try {
		return distributedStoredProcedure(name, args);
    } catch (InterruptedException ex) {
      log.error("Interruption on writeQuery call:", ex.getMessage());
      log.error("trying to get stack trace", ex);
    } catch (ExecutionException ex) {
      log.error("Execution error on writeQuery call:", ex.getMessage());
      log.error("trying to get stack trace", ex);
    }
    return false;
  }

  private boolean distributedStoredProcedure(String name, Map<DMConnId, ByteBuffer> args)
	  throws InterruptedException, ExecutionException {
    List<Future<sproc_result>> futures = new ArrayList<Future<sproc_result>>();
    for (Map.Entry<DMConnId, ByteBuffer>  argsEntry : args.entrySet()) {
      Callable<sproc_result> exec =
          new StoredProcedureExecutor(getSiteManager(argsEntry.getKey()),
                                      clientId, name, argsEntry.getValue());
      Future<sproc_result> f = threadPool.submit(exec);
	  futures.add(f);
    }

	boolean responseOk = true;
	for (Future<sproc_result> f : futures) {
		sproc_result res = f.get();
		if (!checkResponse(res.status, name, "Unable to issue sproc")) {
			responseOk = false;
		}
	}
	return responseOk;
  }


  private boolean storedProcedure(String name, ByteBuffer args, DMSiteManagerClient client) {
    sproc_result result = null;
    StoredProcedureExecutor exec = new StoredProcedureExecutor(client, clientId, name, args);
    result = exec.storedProcedure();
    if (!checkResponse(result.status, name,
                       "Unable to perform stored procedure")) {
      return false;
    }
    return true;
  }

  public boolean storedProcedure(String name, ByteBuffer args) {
	  return storedProcedure(name, args, siteManagerClient);
  }

  public boolean storedProcedure(String name, ByteBuffer args, DMConnId clientId) {
	  return storedProcedure(name, args, getSiteManager(clientId));
  }


  public boolean checkResponse(exec_status_type status, String errMsg,
                               String prefixMsg) {
    boolean response = (status.equals(exec_status_type.PGRES_COMMAND_OK) ||
                        status.equals(exec_status_type.PGRES_TUPLES_OK) ||
                        status.equals(exec_status_type.MASTER_CHANGE_OK));
    if (!response) {
      // sl4j doesn't allow varargs more than 2 parameters in this version
      // for some reason...
      // http://stackoverflow.com/questions/33468151/slf4j-varargs-interprets-first-string-as-marker
      log.warn("{}, response:{}, errMsg:{}",
               new Object[] {prefixMsg, status, errMsg});
    }

    return response;
  }

  public void mergeSVV(List<Integer> toMerge) {
    if (toMerge.size() == 0) {
		return;
	} else if (client_session_version_vector.size() == 0) {
		client_session_version_vector = toMerge;
		return;
	}

	int size = toMerge.size();
	for (int i = 0; i < size; i++) {
		Integer val = Math.max(toMerge.get(i), client_session_version_vector.get(i));
		client_session_version_vector.set(i, val);

	}
  }

  private DMSiteManagerClient getSiteManager(DMConnId smConnId) {
	  try {
		  return openSiteManagerClient(smConnId);
	  } catch (TException t) {
		log.error("Error getting site maanger: {}, {}", smConnId, t.getMessage());
	  }
	  return null;
  }
  private DMSiteManagerClient openSiteManagerClient(DMConnId smConnId)
      throws TException {

	DMSiteManagerClient client;
    long startTime = System.nanoTime();
    client = connPool.getSiteManager(smConnId);
    long endTime = System.nanoTime();
    log.info("Connect to client took:{} ns", (endTime - startTime));
	return client;
  }
}
