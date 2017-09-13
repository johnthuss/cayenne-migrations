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
package org.apache.cayenne.migration;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.cayenne.access.DataNode;
import org.apache.cayenne.configuration.server.ServerRuntime;
import org.apache.cayenne.dba.DbAdapter;
import org.apache.cayenne.dbsync.merge.factory.DefaultMergerTokenFactory;
import org.apache.cayenne.dbsync.merge.factory.MergerTokenFactory;
import org.apache.cayenne.dbsync.merge.token.MergerToken;
import org.apache.cayenne.di.Injector;
import org.apache.cayenne.map.DataMap;
import org.apache.cayenne.merge.ArbitrarySqlToDb;

/**
 * Represents the database and provides operations for changing the schema.
 * 
 * Internally this holds a DataMap and uses it along with a MergerFactory to queue up
 * MergerTokens to perform the schema changes.
 * 
 * @author john
 *
 */
public class MigrationDatabase {

	private List<MergerToken> operations = new ArrayList<MergerToken>();
	private DbAdapter adapter;
	private String databaseProductName;
	private DataMap map = new DataMap("GeneratedMigration");
	private Map<String, MigrationTable> tables = new HashMap<String, MigrationTable>();
	
	MigrationDatabase(DataNode node) {
		this.adapter = node.getAdapter();
	}
	
	/**
	 * Creates a new table in the database.
	 * @param tableName
	 * @return
	 */
	public MigrationTableNew createTable(String tableName) {
		MigrationTable result;
		if (tables.get(tableName) != null) {
			if (!tables.get(tableName).isNew()) {
				throw new IllegalArgumentException(tableName + " is an existing table, it cannot be created.");
			} else {
				throw new IllegalArgumentException(tableName + " has already been created.");
			}
		} else {
			result = new MigrationTableNew(this, tableName);
			tables.put(tableName, result);
		}
		
		return (MigrationTableNew) result;
	}

	/**
	 * Returns an existing table that can be modified.
	 * @param tableName
	 * @return
	 */
	public MigrationTableExisting alterTable(String tableName) {
		MigrationTable result;
		if (tables.get(tableName) != null) {
			result = tables.get(tableName);
			if (result.isNew()) {
				throw new IllegalArgumentException(tableName + " is a new table; it cannot be altered.");
			}
		} else {
			result = new MigrationTableExisting(this, tableName);
			tables.put(tableName, result);
		}
		
		return (MigrationTableExisting) result;
	}
	
	/**
     * Removes a table from the database.
     */
    public void dropTable(String tableName) {
    	MigrationTable table = alterTable(tableName);
        MergerToken op = factory().createDropTableToDb(table.getEntity());
        addOperation(op);
        map.removeDbEntity(tableName);
        tables.remove(tableName);
    }
    
	DataMap getDataMap() {
		return map;
	}

	DbAdapter getAdapter() {
	    return adapter;
	}
	
	MergerTokenFactory factory() {
	    Injector injector = ServerRuntime.getThreadInjector();
	    if (injector != null) {
	        return injector.getInstance(MergerTokenFactory.class);
	    } else {
	        return new DefaultMergerTokenFactory();
	    }
	}

	String getDatabaseProductName() {
	    return databaseProductName;
	}

	void setDatabaseProductName(String databaseProductName) {
	    this.databaseProductName = databaseProductName;
	}

	void addOperation(MergerToken operation) {
		operations.add(operation);
	}
	
	List<MergerToken> getOperations() {
		return operations;
	}

	/**
	 * Add an arbitrary SQL operation to the list of operations to be performed
	 * @param sql
	 */
	public void execute(String sql) {
	    if (sql != null && sql.trim().length() != 0) {
	        addOperation(new ArbitrarySqlToDb(sql));
	    }
	}

}
