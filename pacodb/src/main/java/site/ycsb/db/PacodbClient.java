package site.ycsb.db;

import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URL;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.*;

import com.google.gson.Gson;
import com.google.gson.internal.LinkedTreeMap;
import org.codehaus.jackson.map.ObjectMapper;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;
import site.ycsb.*;

/*
 * Copyright 2013 KU Leuven Research and Development - iMinds - Distrinet
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * 
 * Administrative Contact: dnet-project-office@cs.kuleuven.be
 * Technical Contact: arnaud.schoonjans@student.kuleuven.be
 */
public class PacodbClient extends DB{
	
	// Default configuration
	private static final String DEFAULT_DATABASE_NAME = "usertable";
	private static final String PROTOCOL = "http";

  private static final String HOST_NAME = "http://localhost:49494";

  private static final ObjectMapper mapper = new ObjectMapper();


	// Database connector
	private HttpClient client;
	public PacodbClient(){
		this.client = null;
	}

	// Constructor for testing purposes
	public PacodbClient(List<URL> urls){
		if(urls == null)
			throw new IllegalArgumentException("urls is null");
    client = HttpClient.newHttpClient();
	}
	
	@Override
	public void init() throws DBException{
    client = HttpClient.newHttpClient();
	}
	
	@Override
	public void cleanup() throws DBException {
    // We don't do that here
	}

  // Table variable is not used => already contained in database connector
  @Override
  public Status read(String table, String key, Set<String> fields, Map<String, ByteIterator> result) {
    HttpRequest getRequest = HttpRequest.newBuilder().uri(URI.create(HOST_NAME + "/" + key)).GET().build();
    try {
      HttpResponse<String> response = client.send(getRequest, HttpResponse.BodyHandlers.ofString());

      var body = response.body();
      if(body.isEmpty()) {
        return Status.ERROR;
      } else {
        System.out.println(body);
        return Status.OK;
      }
    } catch (IOException | InterruptedException e) {
      return Status.ERROR;
    }
  }

	@Override
	public Status scan(String table, String startkey, int recordcount,
                     Set<String> fields, Vector<HashMap<String, ByteIterator>> result) {
//		List<StringToStringMap> viewResult = this.executeView(startkey, recordcount);
//		for(StringToStringMap row: viewResult){
//			JSONObject jsonObj = this.parseAsJsonObject(String.valueOf(row));
//			if(jsonObj == null)
//				return Status.ERROR;
//			if(fields == null){
//				@SuppressWarnings("unchecked")
//				Set<String> requestedFields = jsonObj.keySet();
//				result.add(this.getFieldsFromJsonObj(requestedFields, jsonObj));
//			}else{
//				result.add(this.getFieldsFromJsonObj(fields, jsonObj));
//			}
//		}
		return Status.OK;
	}
	private JSONObject parseAsJsonObject(String stringToParse){
		JSONParser parser = new JSONParser();
		try {
			return (JSONObject) parser.parse(stringToParse);
		} catch (ParseException e) {
			e.printStackTrace();
			return null;
		}
	}
	
	// Table variable is not used => already contained in database connector
	@Override
	public Status update(String table, String key,
			Map<String, ByteIterator> values) {
    String body = null;
    try {
      System.out.println(values);
      String requestValue =  mapper.writeValueAsString(values);
      body = String.format("{\"key\":\"%s\", \"value\":%s}", key,requestValue);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
    HttpRequest updateRequest = HttpRequest.newBuilder().uri(URI.create(HOST_NAME + "/"))
        .header("Content-Type", "application/json").PUT(HttpRequest.BodyPublishers.ofString(body)).build();
    return getStatus(updateRequest);
	}
	
	// Table variable is not used => already contained in database connector
// curl -H "Content-Type: application/json" -X POST localhost:49494/ -v -d '{"key":"user6284781860667377211", "value":"all good"}'
	@Override
	public Status insert(String table, String key,
			Map<String, ByteIterator> values) {
        String body = null;
        try {
          System.out.println(values);
          String requestValue =  mapper.writeValueAsString(values);
            body = String.format("{\"key\":\"%s\", \"value\":%s}", key,requestValue);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
  HttpRequest insertRequest = HttpRequest.newBuilder().uri(URI.create(HOST_NAME + "/"))
      .header("Content-Type", "application/json").POST(HttpRequest.BodyPublishers.ofString(body)).build();
    return getStatus(insertRequest);
  }

  // Table variable is not used => already contained in database connector
	@Override
	public Status delete(String table, String key) {
    HttpRequest deleteRequest = HttpRequest.newBuilder().uri(URI.create(HOST_NAME + "/" + key)).DELETE().build();
    return getStatus(deleteRequest);
  }



  private Status getStatus(HttpRequest request) {
    try {
      HttpResponse<String> response = client.send(request, HttpResponse.BodyHandlers.ofString());
      System.out.println(response.body());
      if(response.body() != null) {
        return Status.OK;
      }
    } catch (IOException | InterruptedException e) {
      return Status.ERROR;
    }
    return Status.BAD_REQUEST;
  }
}
