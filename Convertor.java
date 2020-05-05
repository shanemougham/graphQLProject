package SybaseDBConnector;

import java.sql.ResultSet;
import java.util.HashMap;
import java.util.Map;

import org.json.JSONArray;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
/**
 * Utility for converting ResultSets into some Output formats
 */
 public class Convertor {
	 
	static final Logger logger = LoggerFactory.getLogger(Convertor.class.getName());

    /**
     * Convert a result set into a JSON Array
     * @param resultSet
     * @return a JSONArray
     * @throws Exception
     */
	public static String convertToJSON(String QueryString, ResultSet resultSet)
            throws Exception {

    	JSONArray jsonArray = new JSONArray();
        while (resultSet.next()) {
        	JSONObject resObj = new JSONObject();
            int total_rows = resultSet.getMetaData().getColumnCount();
            JSONObject obj = new JSONObject();
            for (int i = 0; i < total_rows; i++) {
            	obj.put(resultSet.getMetaData().getColumnLabel(i + 1)
                        .toLowerCase(), resultSet.getObject(i + 1));
            }
            resObj.put(QueryString,obj);
            jsonArray.put(resObj);
        }
        return jsonArray.toString();
    }
 
    
	public static String[][] buildQuery(Object query)
    {
        ObjectMapper objectMapper = new ObjectMapper();
        JsonNode inputJson = null;
        String tableName,column_value,column_name = null;
        String[][] buildQuery = new String[2][2];
        // Read Event Input Object
        try {
        	inputJson = objectMapper.valueToTree(query);
        	objectMapper.configure(com.fasterxml.jackson.core.JsonParser.Feature.ALLOW_UNQUOTED_FIELD_NAMES, true);
        	
        	int queryCount=0;
        	JsonNode bodyJson = inputJson.get("body");
        	if(bodyJson.get("QueryString") != null) {
        		buildQuery[queryCount][0] = "QueryString";
        		buildQuery[queryCount][1] = bodyJson.get("QueryString").asText();
        		queryCount++;
        	}
        	if(bodyJson.get("QueryTable") != null)
        	{
        		JsonNode tableQuery = bodyJson.get("QueryTable");
	            column_name = tableQuery.get("column_name").asText();
	            tableName = tableQuery.get("table_name").asText();
	            column_value = tableQuery.get("column_value").asText();
        		buildQuery[queryCount][0] = "QueryTable";
	            buildQuery[queryCount][1] = "SELECT * FROM " + tableName + " where " + column_name + "= " +  "'"+ column_value + "'" ;
        		queryCount++;
        	}
        } catch (Exception e) {
            e.printStackTrace();
        }
        
		return buildQuery;
    	
    }


	
    public static GatewayResponse buildResponse(String outputData, int StatusCode) {
    	Map<String, String> headers = new HashMap<>();
        headers.put("Content-Type", "application/json");
        headers.put("X-Custom-Header", "application/json");
  
        return new GatewayResponse(outputData, headers, StatusCode);
    }   
    
    }