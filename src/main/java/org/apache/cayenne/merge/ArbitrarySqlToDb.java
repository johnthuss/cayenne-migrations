/*****************************************************************
 *   Licensed to the Apache Software Foundation (ASF) under one
 *  or more contributor license agreements.  See the NOTICE file
 *  distributed with this work for additional information
 *  regarding copyright ownership.  The ASF licenses this file
 *  to you under the Apache License, Version 2.0 (the
 *  "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing,
 *  software distributed under the License is distributed on an
 *  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 *  KIND, either express or implied.  See the License for the
 *  specific language governing permissions and limitations
 *  under the License.
 ****************************************************************/
package org.apache.cayenne.merge;

import java.util.Collections;
import java.util.List;

import org.apache.cayenne.dba.DbAdapter;

/**
 * A MergerToken that simply executes an explicit sql string using no mapping information (DataMap).<br>
 * This should be considered non-portable since it doesn't rely on the adapter to generate the SQL.
 * However, in theory, standardized SQL statements would work in a portable manner.
 * 
 * @author john
 *
 */
public class ArbitrarySqlToDb extends AbstractToDbToken {

	private String sql;
	
	public ArbitrarySqlToDb(String sql) {
	    super("Arbitrary SQL");
		this.sql = sql;
	}

	public String getTokenValue() {
		return sql;
	}

	public MergerToken createReverse(MergerFactory factory) {
		return new DummyReverseToken(this);
	}

	public int compareTo(MergerToken token) {
		return (token instanceof ArbitrarySqlToDb) ? 0 : 1;
	}

	@Override
	public List<String> createSql(DbAdapter adapter) {
		return Collections.singletonList(sql);
	}

}
