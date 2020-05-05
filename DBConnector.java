package SybaseDBConnector;

import java.io.IOException;
import java.security.InvalidParameterException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Base64;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.amazonaws.services.secretsmanager.AWSSecretsManager;
import com.amazonaws.services.secretsmanager.AWSSecretsManagerClientBuilder;
import com.amazonaws.services.secretsmanager.model.DecryptionFailureException;
import com.amazonaws.services.secretsmanager.model.GetSecretValueRequest;
import com.amazonaws.services.secretsmanager.model.GetSecretValueResult;
import com.amazonaws.services.secretsmanager.model.InternalServiceErrorException;
import com.amazonaws.services.secretsmanager.model.InvalidRequestException;
import com.amazonaws.services.secretsmanager.model.ResourceNotFoundException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

public class DBConnector {

	
	static final Logger logger = LoggerFactory.getLogger(DBConnector.class.getName());
	Connection connection = null;
	
	public DBConnector()
	{
		
	}
	
	public Connection OpenDbConnection()
	{
        // Initialisation Timer
        long start_time = System.nanoTime();
        // Get Secret Credentials
        JsonNode access_keys = null;
        // Register JDBC Driver
        try {
            access_keys = getSecret();
            Class.forName(access_keys.get("driver_name").textValue()).newInstance();
        } catch (InstantiationException e) {
            e.printStackTrace();
        } catch (IllegalAccessException e) {
            e.printStackTrace();
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        } catch (Exception e) {
            e.printStackTrace();
        }
        // Open connection
        try {
            connection = DriverManager.getConnection(access_keys.get("db_url").textValue(),
                    access_keys.get("db_username").textValue(), access_keys.get("db_password").textValue());
            System.out.println("db_url  " + access_keys.get("db_url").textValue());
        
        } catch (Exception e) {
            e.printStackTrace();
        }
                
        long startup_time = System.nanoTime() - start_time;
        System.out.println(String.format("Lambda Initialisation Time = %s nano-seconds.\n", startup_time));

        return connection;
	
	}		
	
    public void CloseDbConnection()
    {
    	try {
			if (connection != null) {
				connection.close();
			}
		} catch (SQLException e) {
			e.printStackTrace();
		}

    }
		
    private static JsonNode getSecret() {

        String secretName = System.getenv("SECRET_KEY");
        ;
        String region = "ap-southeast-2";
        ObjectMapper objectMapper = new ObjectMapper();
        JsonNode secretJson = null;

        // Create a Secrets Manager client
        AWSSecretsManager client = AWSSecretsManagerClientBuilder.standard().withRegion(region).build();

        // In this function we only handle the specific exceptions for the
        // 'GetSecretValue' API.
        // See
        // https://docs.aws.amazon.com/secretsmanager/latest/apireference/API_GetSecretValue.html
        // We rethrow the exception by default.

        String secret, decodedBinarySecret;
        GetSecretValueRequest getSecretValueRequest = new GetSecretValueRequest().withSecretId(secretName);
        GetSecretValueResult getSecretValueResult = null;

        try {
            getSecretValueResult = client.getSecretValue(getSecretValueRequest);
        } catch (DecryptionFailureException e) {
            // Secrets Manager can't decrypt the protected secret text using the provided
            // KMS key.
            // Deal with the exception here, and/or rethrow at your discretion.
            throw e;
        } catch (InternalServiceErrorException e) {
            // An error occurred on the server side.
            // Deal with the exception here, and/or rethrow at your discretion.
            throw e;
        } catch (InvalidParameterException e) {
            // You provided an invalid value for a parameter.
            // Deal with the exception here, and/or rethrow at your discretion.
            throw e;
        } catch (InvalidRequestException e) {
            // You provided a parameter value that is not valid for the current state of the
            // resource.
            // Deal with the exception here, and/or rethrow at your discretion.
            throw e;
        } catch (ResourceNotFoundException e) {
            // We can't find the resource that you asked for.
            // Deal with the exception here, and/or rethrow at your discretion.
            throw e;
        }

        // Decrypts secret using the associated KMS CMK.
        // Depending on whether the secret is a string or binary, one of these fields
        // will be populated.
        if (getSecretValueResult.getSecretString() != null) {
            secret = getSecretValueResult.getSecretString();
        } else {
            decodedBinarySecret = new String(
                    Base64.getDecoder().decode(getSecretValueResult.getSecretBinary()).array());
            secret = String.format("{\"secret_key\": \"%s\"}", decodedBinarySecret);
        }

        // Map Secret Json String to Json Node
        try {
            secretJson = objectMapper.readTree(secret);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return secretJson;
    }
    
		

}
