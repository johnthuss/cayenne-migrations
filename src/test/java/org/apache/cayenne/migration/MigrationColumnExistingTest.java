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
import org.apache.cayenne.dbsync.merge.token.db.SetAllowNullToDb;
import org.apache.cayenne.dbsync.merge.token.db.SetColumnTypeToDb;
import org.apache.cayenne.dbsync.merge.token.db.SetNotNullToDb;
import org.apache.cayenne.dbsync.merge.token.db.SetValueForNullToDb;
import org.apache.cayenne.di.spi.DefaultClassLoaderManager;
import org.apache.cayenne.resource.ClassLoaderResourceLocator;

public class MigrationColumnExistingTest extends TestCase {

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
        MigrationColumn column = table.alterColumn("column");
        assertFalse(column.isNew());
    }

    public void testAddNotNullConstraint() {
        MigrationTableExisting table = db.alterTable("table");
        table.alterColumn("column").addNotNullConstraint();

        assertEquals(1, table.getDatabase().getOperations().size());
        assertTrue(table.getDatabase().getOperations().get(0) instanceof SetNotNullToDb);
        
        SetNotNullToDb operation = (SetNotNullToDb) table.getDatabase().getOperations().get(0);
        assertEquals("column", operation.getColumn().getName());
        assertEquals("table", operation.getColumn().getEntity().getName());
    }

	public void testDropNotNullConstraint() {
	    MigrationTableExisting table = db.alterTable("table");
        table.alterColumn("column").dropNotNullConstraint();

        assertEquals(1, table.getDatabase().getOperations().size());
        assertTrue(table.getDatabase().getOperations().get(0) instanceof SetAllowNullToDb);
        
        SetAllowNullToDb operation = (SetAllowNullToDb) table.getDatabase().getOperations().get(0);
        assertEquals("column", operation.getColumn().getName());
        assertEquals("table", operation.getColumn().getEntity().getName());
    }

	public void testSetDataType() {
	    MigrationTableExisting table = db.alterTable("table");
	    table.alterColumn("column").setDataType(Types.BIGINT);

	    assertEquals(1, table.getDatabase().getOperations().size());
	    assertTrue(table.getDatabase().getOperations().get(0) instanceof SetColumnTypeToDb);

	    SetColumnTypeToDb operation = (SetColumnTypeToDb)table.getDatabase().getOperations().get(0);
	    assertEquals(Types.BIGINT, operation.getColumnNew().getType());
	    assertTrue(table.getDatabase().getOperations().get(0) instanceof SetColumnTypeToDb);
	}

	public void testSetDataTypeWithMaxLength() {
	    MigrationTableExisting table = db.alterTable("table");
        table.alterColumn("column").setDataType(Types.VARCHAR, 256);

        assertEquals(1, table.getDatabase().getOperations().size());
        assertTrue(table.getDatabase().getOperations().get(0) instanceof SetColumnTypeToDb);
        
        SetColumnTypeToDb operation = (SetColumnTypeToDb)table.getDatabase().getOperations().get(0);
        assertEquals(Types.VARCHAR, operation.getColumnNew().getType());
        assertEquals(256, operation.getColumnNew().getMaxLength());
    }

    public void testSetDataTypeWithPrecisionAndScale() {
        MigrationTableExisting table = db.alterTable("table");
        table.alterColumn("column").setDataType(Types.DECIMAL, 38, 4);

        assertEquals(1, table.getDatabase().getOperations().size());
        assertTrue(table.getDatabase().getOperations().get(0) instanceof SetColumnTypeToDb);
        
        SetColumnTypeToDb operation = (SetColumnTypeToDb)table.getDatabase().getOperations().get(0);
        assertEquals(Types.DECIMAL, operation.getColumnNew().getType());
        assertEquals(38, operation.getColumnNew().getAttributePrecision());
        assertEquals(4, operation.getColumnNew().getScale());
    }

	public void testSetDefaultValue() {
	    MigrationTableExisting table = db.alterTable("table");
        table.alterColumn("column").setDefault(1);

        assertEquals(1, table.getDatabase().getOperations().size());
        assertTrue(table.getDatabase().getOperations().get(0) instanceof SetValueForNullToDb);
        
        SetValueForNullToDb operation = (SetValueForNullToDb) table.getDatabase().getOperations().get(0);
        assertEquals("column", operation.getColumn().getName());
        assertEquals("table", operation.getColumn().getEntity().getName());
	}

}
