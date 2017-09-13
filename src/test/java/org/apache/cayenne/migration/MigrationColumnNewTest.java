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

import java.sql.Types;
import java.util.Collections;

import junit.framework.TestCase;

import org.apache.cayenne.access.DataNode;
import org.apache.cayenne.configuration.DefaultRuntimeProperties;
import org.apache.cayenne.configuration.RuntimeProperties;
import org.apache.cayenne.dba.postgres.PostgresAdapter;
import org.apache.cayenne.dbsync.merge.token.db.AddColumnToDb;
import org.apache.cayenne.dbsync.merge.token.db.CreateTableToDb;
import org.apache.cayenne.dbsync.merge.token.db.SetNotNullToDb;
import org.apache.cayenne.dbsync.merge.token.db.SetValueForNullToDb;
import org.apache.cayenne.di.spi.DefaultClassLoaderManager;
import org.apache.cayenne.merge.ArbitrarySqlToDb;
import org.apache.cayenne.resource.ClassLoaderResourceLocator;

public class MigrationColumnNewTest extends TestCase {

	private DataNode node;
	private MigrationDatabase db;
	private MigrationColumnNew subject;
	
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

	public void testAddColumnToNewTableNoDefault() {
		MigrationTable table = db.createTable("table");
		subject = new MigrationColumnNew(table, "column", Types.INTEGER, 0, 0, 0, false, null);

		assertEquals(1, subject.getTable().getDatabase().getOperations().size());
		assertTrue(subject.getTable().getDatabase().getOperations().get(0) instanceof CreateTableToDb);
	}

	public void testAddColumnToNewTableWithDefault() {
		MigrationTable table = db.createTable("table");
		subject = new MigrationColumnNew(table, "column", Types.INTEGER, 0, 0, 0, true, 1);

		assertEquals(2, subject.getTable().getDatabase().getOperations().size());
		assertTrue(subject.getTable().getDatabase().getOperations().get(0) instanceof CreateTableToDb);
	    assertTrue(subject.getTable().getDatabase().getOperations().get(1) instanceof SetValueForNullToDb);

//		CreateTableToDb op = (CreateTableToDb) subject.getTable().getDatabase().getOperations().get(0);
//		List<String> statements = op.createSql(node.getAdapter());
//		for (String statement : statements) {
//            if (statement.toUpperCase().startsWith("CREATE TABLE")) {
//                assertTrue(statement.contains(" DEFAULT "));
//            }
//        }
	}
	
	public void testAddColumnToExistingTableNoDefault() {
		MigrationTable table = db.alterTable("table");
		subject = new MigrationColumnNew(table, "column", Types.INTEGER, 0, 0, 0, false, null);

		assertEquals(1, subject.getTable().getDatabase().getOperations().size());
		assertTrue(subject.getTable().getDatabase().getOperations().get(0) instanceof AddColumnToDb);
	}
	
	public void testAddColumnToExistingTableWithDefault() {
		MigrationTable table = db.alterTable("table");
		subject = new MigrationColumnNew(table, "column", Types.INTEGER, 0, 0, 0, true, 1);

		assertEquals(4, subject.getTable().getDatabase().getOperations().size());
		assertTrue(subject.getTable().getDatabase().getOperations().get(0) instanceof AddColumnToDb);
	    assertTrue(subject.getTable().getDatabase().getOperations().get(1) instanceof SetValueForNullToDb);
	    assertTrue(subject.getTable().getDatabase().getOperations().get(2) instanceof ArbitrarySqlToDb);
        assertTrue(subject.getTable().getDatabase().getOperations().get(3) instanceof SetNotNullToDb);

//		AddColumnToDb op = (AddColumnToDb) subject.getTable().getDatabase().getOperations().get(0);
//		List<String> statements = op.createSql(node.getAdapter());
//		for (String statement : statements) {
//		    assertTrue(statement.contains(" DEFAULT "));
//		}
	}
	
	public void testIsNew() {
	    MigrationTable table = db.alterTable("table");
	    subject = new MigrationColumnNew(table, "column", Types.INTEGER, 0, 0, 0, false, null);
		assertTrue(subject.isNew());
	}

}
