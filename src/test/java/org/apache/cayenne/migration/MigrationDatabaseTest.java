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

import java.util.Collections;

import junit.framework.TestCase;

import org.apache.cayenne.access.DataNode;
import org.apache.cayenne.configuration.DefaultRuntimeProperties;
import org.apache.cayenne.configuration.RuntimeProperties;
import org.apache.cayenne.dba.postgres.PostgresAdapter;
import org.apache.cayenne.dbsync.merge.token.db.CreateTableToDb;
import org.apache.cayenne.dbsync.merge.token.db.DropTableToDb;
import org.apache.cayenne.di.spi.DefaultClassLoaderManager;
import org.apache.cayenne.merge.ArbitrarySqlToDb;
import org.apache.cayenne.resource.ClassLoaderResourceLocator;

public class MigrationDatabaseTest extends TestCase {

    private DataNode node;
    private MigrationDatabase db;
    
    @Override
    protected void setUp() throws Exception {
        super.setUp();
        
        node = new DataNode("node");
        
        RuntimeProperties props = new DefaultRuntimeProperties(Collections.EMPTY_MAP);
        ClassLoaderResourceLocator resourceLocator = new ClassLoaderResourceLocator(new DefaultClassLoaderManager());
        PostgresAdapter adapter = new PostgresAdapter(props, Collections.EMPTY_LIST, Collections.EMPTY_LIST, Collections.EMPTY_LIST, resourceLocator);
        node.setAdapter(adapter);
        
        db = new MigrationDatabase(node);
    }

	public void testAddOperation() {
	    assertEquals(0, db.getOperations().size());
	    db.addOperation(new ArbitrarySqlToDb("UPDATE x SET y=1"));
	    assertEquals(1, db.getOperations().size());
	}

	public void testCreateTable() {
		db.createTable("table");
	    assertEquals(1, db.getOperations().size());
	    assertTrue(db.getOperations().get(0) instanceof CreateTableToDb);
	    
	    CreateTableToDb operation = (CreateTableToDb) db.getOperations().get(0);
	    assertEquals("table", operation.getEntity().getName());
	}

	public void testAlterTable() {
		MigrationTableExisting table = db.alterTable("table");
		MigrationTableExisting table2 = db.alterTable("table");
		assertTrue(table == table2);
	}
	
	public void testCantAlterNewTable() {
		db.createTable("table");
		try {
		    db.alterTable("table");
		    fail("Shouldn't be able to alter a new table.");
		} catch (Exception e) {}
	}

	public void testCantCreateAnExistingTable() {
	    db.alterTable("table");
	    try {
	        db.createTable("table");
	        fail("Shouldn't be able to create an existing table.");
	    } catch (Exception e) {}
	}
	   
	public void testDropTable() {
	    db.dropTable("table");
	    assertEquals(1, db.getOperations().size());
	    assertTrue(db.getOperations().get(0) instanceof DropTableToDb);

	    DropTableToDb operation = (DropTableToDb) db.getOperations().get(0);
	    assertEquals("table", operation.getEntity().getName());
	}
	
	public void testExecute() {
	    db.execute("UPDATE x SET y=1");

	    assertEquals(1, db.getOperations().size());
	    assertTrue(db.getOperations().get(0) instanceof ArbitrarySqlToDb);

	    ArbitrarySqlToDb operation = (ArbitrarySqlToDb) db.getOperations().get(0);
	    assertEquals("UPDATE x SET y=1", operation.getTokenValue());
	}

}
