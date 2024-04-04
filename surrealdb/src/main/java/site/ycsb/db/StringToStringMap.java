package site.ycsb.db;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

import site.ycsb.ByteIterator;
import site.ycsb.StringByteIterator;

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
public class StringToStringMap extends HashMap<String, String> {

	private static final long serialVersionUID = 1L;

	/*
	 * After calling this constructor, the ByteIterator values of otherMap won't
	 * be useful anymore!!!
	 */
	public StringToStringMap(Map<String, ByteIterator> otherMap) {
		super();
		for (String key : otherMap.keySet()) {
			String value = otherMap.get(key).toString();
			super.put(key, value);
		}
	}

	public void put(String key, ByteIterator value) {
		String valueAsString = value.toString();
		super.put(key, valueAsString);
	}

	public ByteIterator getAsByteIt(String key) {
		String value = super.get(key);
		return new StringByteIterator(value);
	}

}