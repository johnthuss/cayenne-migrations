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

import java.sql.Connection;
import java.sql.SQLException;

import org.apache.cayenne.access.DataNode;

/**
 * <p>A migration that relies on an SQL file for it's operations.</p>
 * 
 * The SQL filename follows these naming conventions:<br>
 * 1) It is located in the same package as the Migration class it accompanies.<br>
 * 2) It is named the same as the Migration class is accompanies with a ".sql" extension.<br>
 * Optionally the class name may be followed by a dash ("-") and the database vendor name to refer to a 
 * database-specific file for cases where you want support multiple DB implementations.<br>
 * The database name is determined by the result of: connection.getMetaData().getDatabaseProductName(). See 
 * {@link #databaseSpecificSqlFilename() } for common names.<br>
 * 
 * <p>So for a Migration class named Tutorial0 the generic (universal) sql file would be named 'Tutorial0.sql'<br>
 * For a postgresql-specific migration the sql file would be named 'Tutorial0-PostgreSQL.sql'</p>
 * 
 * @author john
 *
 */
public class SqlFileMigration extends Migration {

	/**
	 * 
	 * @param node the node that you want to apply the migration on
	 */
	public SqlFileMigration(DataNode node) {
		super(node);
	}

	/**
	 * Executes the sql script given by 'sqlFilename()'.
	 */
	@Override
	public void upgrade(MigrationDatabase db) {
		executeSqlScript(sqlFilename());
	}

	/**
	 * Returns the filename for the sql script file for this migration, looking first for a database-specific file, then for a generic one.
	 * @return
	 */
	public String sqlFilename() {
		if (getClass().getResource(databaseSpecificSqlFilename()) != null) {
			return databaseSpecificSqlFilename();
		} else {
			return genericSqlFilename();
		}
	}
	
	/**
	 * Returns the filename for a database-specific sql script file with migration commands. 
	 * For example, for a data map named 'MyDataMap' for the initial migration (zero), the file name would look like this for various databases:<br>
	 * 'MyDataMap0-MicrosoftSQLServer.sql'<br>
	 * 'MyDataMap0-Oracle.sql'<br>
	 * 'MyDataMap0-PostgreSQL.sql'<br>
	 * 'MyDataMap0-MySQL.sql'<br>
	 * <br>
	 * The database name is determined by the result of: connection.getMetaData().getDatabaseProductName()
	 * 
	 * @return
	 */
	protected String databaseSpecificSqlFilename() {
        String databaseProductName = getDatabase().getDatabaseProductName().replace(" ", "");
        return getClass().getSimpleName() + "-" + databaseProductName + ".sql";
	}
	
	/**
	 * Returns the filename for a generic (not database specific) sql script file with migration commands.
	 * @return
	 */
	protected String genericSqlFilename() {
		return getClass().getSimpleName() + ".sql";
	}
	
}
