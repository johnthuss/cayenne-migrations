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
import org.apache.cayenne.access.types.DefaultValueObjectTypeRegistry;
import org.apache.cayenne.configuration.DefaultRuntimeProperties;
import org.apache.cayenne.configuration.RuntimeProperties;
import org.apache.cayenne.dba.postgres.PostgresAdapter;
import org.apache.cayenne.dbsync.merge.token.db.AddRelationshipToDb;
import org.apache.cayenne.dbsync.merge.token.db.CreateTableToDb;
import org.apache.cayenne.dbsync.merge.token.db.SetValueForNullToDb;
import org.apache.cayenne.di.spi.DefaultClassLoaderManager;
import org.apache.cayenne.map.DbAttribute;
import org.apache.cayenne.merge.ArbitrarySqlToDb;
import org.apache.cayenne.resource.ClassLoaderResourceLocator;

public class MigrationTableNewTest extends TestCase {

    private DataNode node;
    private MigrationDatabase db;
    
    @Override
    protected void setUp() throws Exception {
        super.setUp();
        
        node = new DataNode("node");
        
        RuntimeProperties props = new DefaultRuntimeProperties(Collections.EMPTY_MAP);
        ClassLoaderResourceLocator resourceLocator = new ClassLoaderResourceLocator(new DefaultClassLoaderManager());
        PostgresAdapter adapter = new PostgresAdapter(props, Collections.EMPTY_LIST, Collections.EMPTY_LIST, Collections.EMPTY_LIST, resourceLocator, new DefaultValueObjectTypeRegistry(Collections.emptyList()));
        node.setAdapter(adapter);
        
        db = new MigrationDatabase(node);
    }

	public void testMigrationTableNew() {
        MigrationTableNew table = db.createTable("table");
        assertEquals(1, table.getDatabase().getOperations().size());
        assertTrue(table.getDatabase().getOperations().get(0) instanceof CreateTableToDb);
	}

	public void testIsNew() {
	    MigrationTableNew table = db.createTable("table");
	    assertTrue(table.isNew());
	}
	
	public void testAddColumn() {
		MigrationTableNew table = db.createTable("table");
	    table.addColumn("column", Types.INTEGER);
	    assertEquals(1, table.getDatabase().getOperations().size());
	    assertTrue(table.getDatabase().getOperations().get(0) instanceof CreateTableToDb);
	}

    public void testAddColumnWithDefault() {
        MigrationTableNew table = db.createTable("table");
        table.addColumn("column", Types.INTEGER, true, 1);
        assertEquals(2, table.getDatabase().getOperations().size());
        assertTrue(table.getDatabase().getOperations().get(0) instanceof CreateTableToDb);
        assertTrue(table.getDatabase().getOperations().get(1) instanceof SetValueForNullToDb);

//        CreateTableToDb op = (CreateTableToDb) table.getDatabase().getOperations().get(0);
//        List<String> statements = op.createSql(node.getAdapter());
//        for (String statement : statements) {
//            if (statement.toUpperCase().startsWith("CREATE TABLE")) {
//                assertTrue(statement.contains(" DEFAULT "));
//            }
//        }
    }
	    
	public void testAddPrimaryKey() {
        MigrationTableNew table = db.createTable("table");
        table.addIntegerColumn("pk");
        table.addPrimaryKey("pk");
        
        assertEquals(1, table.getDatabase().getOperations().size());
        assertTrue(table.getDatabase().getOperations().get(0) instanceof CreateTableToDb);
        DbAttribute pk = table.getEntity().getAttribute("pk");
        assertTrue(pk.isPrimaryKey());
	}
	
	public void testAddForeignKey() {
        MigrationTableNew table = db.createTable("table");
        table.addForeignKey("fk", "table2", "pk");
        
        assertEquals(2, table.getDatabase().getOperations().size());
        assertTrue(table.getDatabase().getOperations().get(0) instanceof CreateTableToDb);
        assertTrue(table.getDatabase().getOperations().get(1) instanceof AddRelationshipToDb);
        
        // FIXME getRelationship() does no longer exist on AddRelationshipToDb
//        AddRelationshipToDb operation = (AddRelationshipToDb) table.getDatabase().getOperations().get(1);
//        DbRelationship relationship = operation.getRelationship();
//        assertEquals("table2", relationship.getTargetEntityName());
//        assertEquals("fk", relationship.getJoins().get(0).getSource().getName());
//        assertEquals("pk", relationship.getJoins().get(0).getTarget().getName());
	}
	
   public void testAddForeignKeyAndIndex() {
        MigrationTableNew table = db.createTable("table");
        table.addForeignKey("fk", "table2", "pk", true);
        
        assertEquals(3, table.getDatabase().getOperations().size());
        assertTrue(table.getDatabase().getOperations().get(0) instanceof CreateTableToDb);
        assertTrue(table.getDatabase().getOperations().get(1) instanceof ArbitrarySqlToDb);
        assertTrue(table.getDatabase().getOperations().get(2) instanceof AddRelationshipToDb);

        // FIXME getRelationship() does no longer exist on AddRelationshipToDb
//        AddRelationshipToDb operation = (AddRelationshipToDb) table.getDatabase().getOperations().get(2);
//        DbRelationship relationship = operation.getRelationship();
//        assertEquals("table2", relationship.getTargetEntityName());
//        assertEquals("fk", relationship.getJoins().get(0).getSource().getName());
//        assertEquals("pk", relationship.getJoins().get(0).getTarget().getName());
    }

}
