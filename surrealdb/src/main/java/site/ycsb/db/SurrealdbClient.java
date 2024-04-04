package site.ycsb.db;

import java.net.MalformedURLException;
import java.net.URL;
import java.util.*;

import com.google.gson.internal.LinkedTreeMap;
import com.surrealdb.connection.SurrealWebSocketConnection;
import com.surrealdb.driver.SyncSurrealDriver;
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
public class SurrealdbClient extends DB{
	
	// Default configuration
	private static final String DEFAULT_DATABASE_NAME = "usertable";
	private static final String PROTOCOL = "http";
	// Database connector
	private SurrealWebSocketConnection dbConnector;
  private SyncSurrealDriver driver;
	public SurrealdbClient(){
		this.dbConnector = null;
	}

	// Constructor for testing purposes
	public SurrealdbClient(List<URL> urls){
		if(urls == null)
			throw new IllegalArgumentException("urls is null");
		this.dbConnector = new SurrealWebSocketConnection("localhost", 8000, false);
    this.dbConnector.connect(5);
    this.driver = new SyncSurrealDriver(dbConnector);
	}
	
	@Override
	public void init() throws DBException{
    this.dbConnector = new SurrealWebSocketConnection("localhost", 8000, false);
    this.dbConnector.connect(5);
    this.driver = new SyncSurrealDriver(dbConnector);
    this.driver.signIn("root", "root");
    this.driver.use("ycsb-test", DEFAULT_DATABASE_NAME);
	}
	
	@Override
	public void cleanup() throws DBException {
    this.dbConnector.disconnect();
	}

	private LinkedTreeMap executeReadOperation(String key){
        return this.driver.select("%s:%s".formatted(DEFAULT_DATABASE_NAME, key), LinkedTreeMap.class).isEmpty() ? null : this.driver.select("%s:%s".formatted(DEFAULT_DATABASE_NAME, key), LinkedTreeMap.class).get(0);
	}
	
	private Status executeWriteOperation(String key, LinkedTreeMap dataToWrite){
			dataToWrite.put("id", key);
    this.driver.create(DEFAULT_DATABASE_NAME, dataToWrite);
		return Status.OK;
	}
	
	private Status executeDeleteOperation(String dataToDelete){
			this.driver.delete(dataToDelete);
		return Status.OK;
	}
	
	private Status executeUpdateOperation(String key, LinkedTreeMap dataToUpdate){
    this.driver.update(key, dataToUpdate);
		return Status.OK;
	}
	
	private void copyRequestedFieldsToResultMap(Set<String> fields,
                                              LinkedTreeMap inputMap,
			Map<String, ByteIterator> result){
		for(String field: fields){
			Object value = inputMap.get(field);
			result.put(field, new StringByteIterator(value.toString()));
		}
		ByteIterator _id = new StringByteIterator(inputMap.get("id").toString());
		ByteIterator _rev = new StringByteIterator(inputMap.get("rev").toString());
		result.put("id",  _id);
		result.put("rev", _rev);
	}
	
	private void copyAllFieldsToResultMap(LinkedTreeMap inputMap,
			Map<String, ByteIterator> result){
		for(Object field: inputMap.keySet()){
			ByteIterator value = new StringByteIterator(inputMap.get(field).toString());
			result.put( field.toString(), value);
		}
	}

  // Table variable is not used => already contained in database connector
  @Override
  public Status read(String table, String key, Set<String> fields, Map<String, ByteIterator> result) {
    LinkedTreeMap queryResult = this.executeReadOperation(key);
    if(queryResult == null)
      return Status.ERROR;
    if(fields == null){
      this.copyAllFieldsToResultMap(queryResult, result);
    }else{
      this.copyRequestedFieldsToResultMap(fields, queryResult, result);
    }
    return Status.OK;
  }

	@Override
	public Status scan(String table, String startkey, int recordcount,
                     Set<String> fields, Vector<HashMap<String, ByteIterator>> result) {
		List<StringToStringMap> viewResult = this.executeView(startkey, recordcount);
		for(StringToStringMap row: viewResult){
			JSONObject jsonObj = this.parseAsJsonObject(String.valueOf(row));
			if(jsonObj == null)
				return Status.ERROR;
			if(fields == null){
				@SuppressWarnings("unchecked")
				Set<String> requestedFields = jsonObj.keySet();
				result.add(this.getFieldsFromJsonObj(requestedFields, jsonObj));
			}else{
				result.add(this.getFieldsFromJsonObj(fields, jsonObj));
			}
		}
		return Status.OK;
	}

  private List<StringToStringMap> executeView(String startKey, int amountOfRecords){
    List<StringToStringMap> records = this.driver.select(DEFAULT_DATABASE_NAME, StringToStringMap.class);
    return records;
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
	
	private HashMap<String, ByteIterator> getFieldsFromJsonObj(Set<String> fields, JSONObject jsonObj){
		HashMap<String, ByteIterator> result = new HashMap<String, ByteIterator>();
		for(String key: fields){
			String value = jsonObj.get(key).toString();
			result.put(key, new StringByteIterator(value));
		}
		return result;
	}
	
	// Table variable is not used => already contained in database connector
	@Override
	public Status update(String table, String key,
			Map<String, ByteIterator> values) {
    LinkedTreeMap queryResult = this.executeReadOperation(key);
		if(queryResult == null)
			return Status.ERROR;
    LinkedTreeMap updatedMap = this.updateFields(queryResult, values);
		return this.executeUpdateOperation(key, updatedMap);
	}

	private LinkedTreeMap updateFields(LinkedTreeMap toUpdate,
							Map<String, ByteIterator> newValues){
		for(String updateField: newValues.keySet()){
			ByteIterator newValue = newValues.get(updateField);
			toUpdate.put(updateField, newValue);
		}
		return toUpdate;
	}
	
	// Table variable is not used => already contained in database connector
	@Override
	public Status insert(String table, String key,
			Map<String, ByteIterator> values) {
    LinkedTreeMap dataToInsert = (LinkedTreeMap) Map.copyOf(values);
		return this.executeWriteOperation(key, dataToInsert);
	}

	// Table variable is not used => already contained in database connector
	@Override
	public Status delete(String table, String key) {
    LinkedTreeMap toDelete = this.executeReadOperation(key);
		if(toDelete == null)
			return Status.ERROR;
		return this.executeDeleteOperation(key);
	}
	
}
