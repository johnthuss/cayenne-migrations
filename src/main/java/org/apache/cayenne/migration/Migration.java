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

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;

import org.apache.cayenne.access.DataNode;

/**
 * <p>Abstract class for defining database migrations. Migrations are used to create a database
 * schema initially and to update the schema over time while keeping track of the specific versions.</p>
 * 
 * Subclasses must implement upgrade to perform the operations necessary to change the schema. For example:
 * 
 * <code><pre>
 * public class Tutorial0 extends Migration {
 *	
 *   public Tutorial0(DataNode node) {
 *     super(node);
 *   }
 *
 *   public void upgrade(MigrationDatabase db) {
 *     MigrationTableNew artist = db.createTable("Artist");
 *     cardProcessor.addIntegerColumn("artist_id", MANADATORY, null);
 *     cardProcessor.addVarcharColumn("name", MANADATORY, null);
 *     cardProcessor.addDateColumn("date_of_birth");
 *     cardProcessor.addPrimaryKey("cardProcessor_id");
 *     
 *     MigrationTableExisting painting = db.alterTable("Painting");
 *     painting.alterColumn("price").drop();
 *   }
 * }
 * </pre></code>
 * 
 * <p>The initial migration to create a database for the first time can be generated automatically
 * by using {@link org.apache.cayenne.migration.MigrationGenerator}.</p>
 * 
 * @author john
 *
 */
public abstract class Migration {

    protected static final boolean MANDATORY = true;
    protected static final boolean OPTIONAL = false;

	private final DataNode node;
	private final MigrationDatabase database;
	
	public Migration(DataNode node) {
		this.node = node;
		this.database = new MigrationDatabase(node);
	}

	public DataNode getDataNode() {
		return node;
	}
	
	/**
	 * Subclasses needs to override this and perform databases by using the passed in db object, or by executing SQL directly.
	 * 
	 * @param db
	 */
	public abstract void upgrade(MigrationDatabase db);
	
	void run() {
		upgrade(database);
	}
	
	MigrationDatabase getDatabase() {
		return database;
	}

//	void executeOperations() {
//		MergerContext ctx = new ExecutingMergerContext(database.getDataMap(), node);
//
//		for (MergerToken token : database.getOperations()) {
//			token.execute(ctx);
//		}
//	}
	
	/**
	 * Adds an explicit SQL statement to the queue of operations. 
	 */
	public void executeSqlStatement(String sql) {
		database.execute(sql);
	}
	
	/**
	 * Adds sql from a file in the classpath to the queue of operations. 
	 * 
	 * @param filename
	 */
	public void executeSqlScript(String filename) {
		String sql;
		try {
			sql = loadTextResource(filename, getClass());
		} catch (IOException e) {
			throw new RuntimeException("Unable to load sql script '" + filename + "' for migration: " + getClass().getSimpleName(), e);
		}
		
		executeSqlStatement(sql);
	}
	
	/**
	 * Loads a text file in the classpath.
	 * 
	 * @param filename
	 * @param clazz
	 * @return
	 * @throws IOException
	 */
	protected static String loadTextResource(String filename, Class<?> clazz) throws IOException {
        StringBuffer buffer = new StringBuffer();
        
        InputStream stream = clazz.getResourceAsStream(filename);
        if (stream == null) {
            throw new FileNotFoundException(filename);
        }
        
        try {
            Reader reader = new InputStreamReader(stream, "UTF-8");
            try {
                int character;
                while ((character = reader.read()) != -1) {
                    buffer.append((char)character);
                }
            } finally {
                reader.close();
            }
        } finally {
            stream.close();
        }
        
        return buffer.toString();
    }
	
}
