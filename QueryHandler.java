package SybaseDBConnector;


import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;




/**
 * Handler for requests to Lambda function.
 */
public class QueryHandler implements RequestHandler<Object, Object> {

	static final Logger logger = LoggerFactory.getLogger(QueryHandler.class.getName());
	DBConnector driverManager = new DBConnector();
	Connection connection = null;

    public QueryHandler() {
    	connection = driverManager.OpenDbConnection(); 
       }
	
	
    public Object handleRequest(final Object input, final Context context) {
    
	    Statement selectStmt = null;
	    ResultSet queryResultSet = null;
	    GatewayResponse response = null;
	    String[][] queryToExecute = null;
	    String queryString = null;
	    String outputJson = null;
	    try {   
	    	
	    	    if(connection.isClosed())
	    	    	connection = driverManager.OpenDbConnection(); 
	    	    	
	    	    selectStmt = connection.createStatement();
				queryToExecute = Convertor.buildQuery(input);
				for (int i = 0; i < queryToExecute.length; i++) {
					queryString = queryToExecute[i][0];
					queryResultSet = selectStmt.executeQuery(queryToExecute[i][1]);
					outputJson = outputJson + Convertor.convertToJSON(queryString,queryResultSet);
				}
				response = Convertor.buildResponse(outputJson,200);
		     } catch(Exception ex) {
		            ex.printStackTrace();
		            response = Convertor.buildResponse("",500);
	            } 
	    return response;
    }
  
}
