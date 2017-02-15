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
import org.apache.cayenne.di.spi.DefaultClassLoaderManager;
import org.apache.cayenne.map.DataMap;
import org.apache.cayenne.resource.ClassLoaderResourceLocator;

// TODO: implement tests for Migrator - not sure how to connect to a real database
public class MigratorTest extends TestCase {

    private DataNode node;
//    private MockConnection connection;
    
    @Override
    protected void setUp() throws Exception {
        super.setUp();
        
        node = new DataNode("node");
        
        RuntimeProperties props = new DefaultRuntimeProperties(Collections.EMPTY_MAP);
        ClassLoaderResourceLocator resourceLocator = new ClassLoaderResourceLocator(new DefaultClassLoaderManager());
        PostgresAdapter adapter = new PostgresAdapter(props, Collections.EMPTY_LIST, Collections.EMPTY_LIST, Collections.EMPTY_LIST, resourceLocator);
        node.setAdapter(adapter);
        
        DataMap map = new DataMap("MyMap");
        node.addDataMap(map);
        
//        connection = new MockConnection();
//        
//        MockDataSource dataSource = new MockDataSource();
//        dataSource.setupConnection(connection);
//        node.setDataSource(dataSource);
    }
    
    public void testSomething() {
    }
    
//    
//    public void testCreateMigrationClassForVersion() {
//        Migrator migrator = new Migrator(node, getClass().getPackage().getName());
//        DataMap map = new DataMap("MyMap");
//        
//        Migration migration0 = migrator.createMigrationClassForVersion(map, 0);
//        assertNotNull(migration0);
//        assertNotNull(migration0 instanceof MyMap0);
//    }
//    
//    public void testExecuteSqlWithUpdateCount() {
//        Migrator migrator = new Migrator(node, getClass().getPackage().getName());
//        try {
//            migrator.executeSqlWithUpdateCount("UPDATE x SET y=1");
//        } catch (SQLException e) {
//            fail(e.getMessage());
//        }
//    }
//    
//    public void testExecuteSqlWithUpdateCountList() {
//        Migrator migrator = new Migrator(node, getClass().getPackage().getName());
//        try {
//            migrator.executeSqlWithUpdateCount(Collections.singletonList("UPDATE x SET y=1"));
//        } catch (SQLException e) {
//            fail(e.getMessage());
//        }
//    }
//    
//    public void testExecuteSqlReturnInt() {
//        Migrator migrator = new Migrator(node, getClass().getPackage().getName());
//        try {
//            migrator.executeSqlReturnInt("SELECT y FROM x");
//        } catch (SQLException e) {
//            fail(e.getMessage());
//        }
//    }
//    
//    public void testExecuteOperations() {
//        Migrator migrator = new Migrator(node, getClass().getPackage().getName());
//        try {
//            MigrationDatabase db = new MigrationDatabase(node);
//            MigrationTable table = db.createTable("MyTable");
//            table.addIntegerColumn("pk");
//            table.addPrimaryKey("pk");
//            migrator.executeOperations(db.getOperations());
//        } catch (SQLException e) {
//            fail(e.getMessage());
//        } 
//    }
//    
//    public void testCreateInternalMigrationSchema() throws SQLException {
//        Migrator migrator = new Migrator(node, getClass().getPackage().getName());
//        migrator.createInternalMigrationSchema();
//    }
//    
//    public void testCurrentDbVersion() throws SQLException {
//        Migrator migrator = new Migrator(node, getClass().getPackage().getName());
//        DataMap map = node.getDataMaps().iterator().next();
//        migrator.currentDbVersion(map);
//    }
//    
//    public void testSetDbVersion() throws SQLException {
//        Migrator migrator = new Migrator(node, getClass().getPackage().getName());
//        DataMap map = node.getDataMaps().iterator().next();
//        migrator.setDbVersion(map, 0);
//    }
//    
//    public void testLock() throws SQLException {
//        Migrator migrator = new Migrator(node, getClass().getPackage().getName());
//        DataMap map = node.getDataMaps().iterator().next();
//        migrator.lock(map);
//    }
//    
//    public void testLockTwiceFails() throws SQLException {
//        Migrator migrator = new Migrator(node, getClass().getPackage().getName());
//        DataMap map = node.getDataMaps().iterator().next();
//        assertTrue(migrator.lock(map));
//        assertFalse(migrator.lock(map));
//    }
//    
//    public void testUnlock() throws SQLException {
//        Migrator migrator = new Migrator(node, getClass().getPackage().getName());
//        DataMap map = node.getDataMaps().iterator().next();
//        migrator.lock(map);
//        migrator.unlock(map);
//    }
//    
//    public void testUnlockTwiceFails() throws SQLException {
//        Migrator migrator = new Migrator(node, getClass().getPackage().getName());
//        DataMap map = node.getDataMaps().iterator().next();
//        migrator.lock(map);
//        migrator.unlock(map);
//        try {
//            migrator.unlock(map);
//            fail("Unlocking twice should fail");
//        } catch (Exception e) {}
//    }
//    
//    public void testMigrateToLatest() throws SQLException {
//		Migrator migrator = new Migrator(node, getClass().getPackage().getName());
//        migrator.migrateToLatest();
//	}

}
