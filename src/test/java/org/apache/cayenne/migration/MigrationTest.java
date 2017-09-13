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

import java.io.IOException;
import java.util.Collections;

import junit.framework.TestCase;

import org.apache.cayenne.access.DataNode;
import org.apache.cayenne.access.types.DefaultValueObjectTypeRegistry;
import org.apache.cayenne.configuration.DefaultRuntimeProperties;
import org.apache.cayenne.configuration.RuntimeProperties;
import org.apache.cayenne.dba.postgres.PostgresAdapter;
import org.apache.cayenne.di.spi.DefaultClassLoaderManager;
import org.apache.cayenne.merge.ArbitrarySqlToDb;
import org.apache.cayenne.resource.ClassLoaderResourceLocator;

public class MigrationTest extends TestCase {
    
    private DataNode node;
    
    @Override
    protected void setUp() throws Exception {
        super.setUp();
        
        node = new DataNode("node");
        
        RuntimeProperties props = new DefaultRuntimeProperties(Collections.EMPTY_MAP);
        ClassLoaderResourceLocator resourceLocator = new ClassLoaderResourceLocator(new DefaultClassLoaderManager());
        PostgresAdapter adapter = new PostgresAdapter(props, Collections.EMPTY_LIST, Collections.EMPTY_LIST, Collections.EMPTY_LIST, resourceLocator, new DefaultValueObjectTypeRegistry(Collections.emptyList()));
        node.setAdapter(adapter);
    }
    
    private static class MyMigration extends Migration {
        public MyMigration(DataNode node) {
            super(node);
        }
        @Override
        public void upgrade(MigrationDatabase db) {}    
    }
    
	public void testExecuteSqlStatement() {
	    MyMigration migration = new MyMigration(node);
        migration.executeSqlStatement("UPDATE x SET y=1");

        assertEquals(1, migration.getDatabase().getOperations().size());
        assertTrue(migration.getDatabase().getOperations().get(0) instanceof ArbitrarySqlToDb);

        ArbitrarySqlToDb operation = (ArbitrarySqlToDb) migration.getDatabase().getOperations().get(0);
        assertEquals("UPDATE x SET y=1", operation.getTokenValue());
    }

	public void testLoadTextResource() {
	    try {
            String sql = Migration.loadTextResource("testMigrationScript.sql", getClass());
            assertEquals("UPDATE x SET y=1;", sql);
        } catch (IOException e) {
            fail(e.getMessage());
        }
	}

	public void testExecuteSqlScript() {
        MyMigration migration = new MyMigration(node);
        migration.executeSqlScript("testMigrationScript.sql");

        assertEquals(1, migration.getDatabase().getOperations().size());
        assertTrue(migration.getDatabase().getOperations().get(0) instanceof ArbitrarySqlToDb);

        ArbitrarySqlToDb operation = (ArbitrarySqlToDb) migration.getDatabase().getOperations().get(0);
        assertEquals("UPDATE x SET y=1;", operation.getTokenValue());
    }

}
