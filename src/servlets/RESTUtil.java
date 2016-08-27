package servlets;

import java.sql.Date;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.Instant;
import java.time.LocalDateTime;
import java.util.Calendar;
import javax.ws.rs.core.MediaType;

import org.apache.commons.lang3.StringUtils;
import org.codehaus.jettison.json.JSONArray;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;

import com.sun.jersey.api.client.ClientHandlerException;
import com.sun.jersey.api.client.ClientResponse;
import com.sun.jersey.api.client.WebResource.Builder;
import com.sun.jersey.api.client.Client;

public class RESTUtil {

    private static final String SQL_VARIABLE = "?";


    public static void waitNanoSeconds(long duration) {
        long start = System.nanoTime();
        while (start + duration > System.nanoTime()) {

        }
    }

    public static void waitMilliSeconds(long duration) {
        long start = System.currentTimeMillis();
        while (start + duration > System.currentTimeMillis()) {

        }
    }

    public static Builder makeRestConnection(long terminalID) {
        Client client = new Client();
        String path = "http://54.165.30.77:8080/kronos/rest/query";
        // TODO: @anilpacaci Kronos needs this to differentiate different threads
        path = path + '/' + terminalID;
        System.out.println(path);
        Builder builder = client.resource(path).accept(MediaType.APPLICATION_JSON).type(MediaType.APPLICATION_JSON);
        return builder;
    }


    public static Integer executeUpdateQuery(Builder builder, String sqlStringWithVariables, String... replacements) throws SQLException {

        String sqlQuery = "{ \"query\": \"" + sqlStringWithVariables + "\" }";

        for (int i = 0; i < replacements.length; i++) {
            sqlQuery = StringUtils.replaceOnce(sqlQuery, SQL_VARIABLE, replacements[i]);
        }

        ClientResponse response = RESTUtil.getClient(builder).post(ClientResponse.class, sqlQuery);
        if (response.getClientResponseStatus() != com.sun.jersey.api.client.ClientResponse.Status.OK) {
            throw new SQLException("Query " + sqlQuery + " encountered an error ");
        }

        Integer result = Integer.valueOf(response.getEntity(String.class));
        response.close();

        return result;

    }

    public static JSONArray executeSelectQuery(Builder builder, String sqlStringWithVariables, String... replacements) throws SQLException {

        String sqlQuery = "{ \"query\": \"" + sqlStringWithVariables + "\" }";

        for (int i = 0; i < replacements.length; i++) {
            sqlQuery = StringUtils.replaceOnce(sqlQuery, SQL_VARIABLE, replacements[i]);
        }

        ClientResponse response = RESTUtil.getClient(builder).post(ClientResponse.class, sqlQuery);
        if (response.getClientResponseStatus() != com.sun.jersey.api.client.ClientResponse.Status.OK) {
            throw new SQLException("Query " + sqlQuery + " encountered an error ");
        }

        JSONArray jsonArray = response.getEntity(JSONArray.class);
        response.close();

        return jsonArray;

    }


    public static Builder getClient(Builder builder) {
        if (builder == null) {
            new ClientHandlerException("No REST Client, request could not be issued");
        }
        return builder.accept(MediaType.APPLICATION_JSON).type(MediaType.APPLICATION_JSON);
    }

}
