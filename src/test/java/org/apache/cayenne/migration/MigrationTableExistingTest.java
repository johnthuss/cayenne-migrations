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
import org.apache.cayenne.dbsync.merge.token.db.AddColumnToDb;
import org.apache.cayenne.dbsync.merge.token.db.AddRelationshipToDb;
import org.apache.cayenne.dbsync.merge.token.db.DropColumnToDb;
import org.apache.cayenne.dbsync.merge.token.db.DropRelationshipToDb;
import org.apache.cayenne.dbsync.merge.token.db.SetNotNullToDb;
import org.apache.cayenne.dbsync.merge.token.db.SetPrimaryKeyToDb;
import org.apache.cayenne.dbsync.merge.token.db.SetValueForNullToDb;
import org.apache.cayenne.di.spi.DefaultClassLoaderManager;
import org.apache.cayenne.map.DbAttribute;
import org.apache.cayenne.merge.ArbitrarySqlToDb;
import org.apache.cayenne.resource.ClassLoaderResourceLocator;

public class MigrationTableExistingTest extends TestCase {
    
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

	public void testIsNew() {
        MigrationTableExisting table = db.alterTable("table");
        assertFalse(table.isNew());
	}
	
	public void testMigrationTableExisting() {
        MigrationTableExisting table = db.alterTable("table");
        assertEquals(0, table.getDatabase().getOperations().size());
    }

	public void testDropColumn() {
        MigrationTableExisting table = db.alterTable("table");
        table.dropColumn("column");
        assertEquals(1, table.getDatabase().getOperations().size());
        assertTrue(table.getDatabase().getOperations().get(0) instanceof DropColumnToDb);
	}
	
	public void testAddColumn() {
        MigrationTableExisting table = db.alterTable("table");
        table.addColumn("column", Types.INTEGER);
        assertEquals(1, table.getDatabase().getOperations().size());
        assertTrue(table.getDatabase().getOperations().get(0) instanceof AddColumnToDb);
        
        AddColumnToDb operation = (AddColumnToDb) table.getDatabase().getOperations().get(0);
        assertEquals("column", operation.getColumn().getName());
        assertEquals(Types.INTEGER, operation.getColumn().getType());
        assertEquals("table", operation.getColumn().getEntity().getName());
        assertFalse(operation.getColumn().isMandatory());
	}
	
	public void testAddColumnWithDefault() {
	    MigrationTableExisting table = db.alterTable("table");
	    table.addColumn("column", Types.INTEGER, true, 1);
	    assertEquals(4, table.getDatabase().getOperations().size());
	    assertTrue(table.getDatabase().getOperations().get(0) instanceof AddColumnToDb);
	    assertTrue(table.getDatabase().getOperations().get(1) instanceof SetValueForNullToDb);
	    assertTrue(table.getDatabase().getOperations().get(2) instanceof ArbitrarySqlToDb);
        assertTrue(table.getDatabase().getOperations().get(3) instanceof SetNotNullToDb);

        AddColumnToDb operation = (AddColumnToDb) table.getDatabase().getOperations().get(0);
        assertEquals("column", operation.getColumn().getName());
        assertEquals(Types.INTEGER, operation.getColumn().getType());
        assertEquals("table", operation.getColumn().getEntity().getName());
        assertTrue(operation.getColumn().isMandatory());
	}

	public void testAddPrimaryKey() {
        MigrationTableExisting table = db.alterTable("table");
        table.addIntegerColumn("pk");
        table.addPrimaryKey("pk");
        assertEquals(2, table.getDatabase().getOperations().size());
        assertTrue(table.getDatabase().getOperations().get(0) instanceof AddColumnToDb);
        assertTrue(table.getDatabase().getOperations().get(1) instanceof SetPrimaryKeyToDb);
        
        DbAttribute pk = table.getEntity().getAttribute("pk");
        assertTrue(pk.isPrimaryKey());
	}

	public void testAddForeignKey() {
	    MigrationTableExisting table = db.alterTable("table");
	    table.addForeignKey("fk", "table2", "pk");

	    assertEquals(1, table.getDatabase().getOperations().size());
	    assertTrue(table.getDatabase().getOperations().get(0) instanceof AddRelationshipToDb);

	    // FIXME getRelationship() does no longer exist on AddRelationshipToDb
//	    AddRelationshipToDb operation = (AddRelationshipToDb) table.getDatabase().getOperations().get(0);
//	    DbRelationship relationship = operation.getRelationship();
//	    assertEquals("table2", relationship.getTargetEntityName());
//	    assertEquals("fk", relationship.getJoins().get(0).getSource().getName());
//	    assertEquals("pk", relationship.getJoins().get(0).getTarget().getName());
	}
	   
	public void testDropForeignKey() {
        MigrationTableExisting table = db.alterTable("table");
        table.dropForeignKey("fk", "table2", "pk");
        assertEquals(1, table.getDatabase().getOperations().size());
        assertTrue(table.getDatabase().getOperations().get(0) instanceof DropRelationshipToDb);
	}
	
	public void testAlterColumn() {
        MigrationTableExisting table = db.alterTable("table");
        table.alterColumn("column").addNotNullConstraint();
        
        // ensure alterColumn can be called multiple times without error
        table.alterColumn("column").setDefault(1);
        
        assertEquals(1, table.getEntity().getAttributes().size());
        assertEquals(2, table.getDatabase().getOperations().size());
	}
	
	public void testCantAlterNewColumn() {
	    MigrationTableExisting table = db.alterTable("table");
	    table.addIntegerColumn("column");
	    try {
	        table.alterColumn("column");
	        fail("New columns should not be allowed to altered.");
	    } catch (Exception e) {
	    }
	}
	
	public void testCantCreateAnExistingColumn() {
	    MigrationTableExisting table = db.alterTable("table");
	    table.alterColumn("column");
	    try {
	        table.addIntegerColumn("column");
	        fail("Existing columns should not be allowed to created.");
	    } catch (Exception e) {
	    }
	}

}
