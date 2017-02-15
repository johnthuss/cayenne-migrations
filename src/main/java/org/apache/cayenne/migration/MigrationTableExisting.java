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

import org.apache.cayenne.map.DbAttribute;
import org.apache.cayenne.merge.MergerToken;

/**
 * Represents an existing table in the database and provides operations for changing the schema.
 * 
 * Internally this holds a DbEntity and uses it along with a MergerFactory to queue up
 * MergerTokens to perform the schema changes.
 * 
 * @author john
 *
 */
public class MigrationTableExisting extends MigrationTable {

	MigrationTableExisting(MigrationDatabase database, String tableName) {
		super(database, tableName);
	}

	@Override
	boolean isNew() {
		return false;
	}
	
    /**
     * Returns an existing column that can be changed.
     * @param columnName
     * @return
     */
    public MigrationColumnExisting alterColumn(String columnName) {
        MigrationColumn result;
        if (getColumns().get(columnName) != null) {
            result = getColumns().get(columnName);
            if (result.isNew()) {
                throw new IllegalArgumentException(columnName + " is a new column, it cannot be altered.");
            }
        } else {
            result = new MigrationColumnExisting(this, columnName);
        }
        
        return (MigrationColumnExisting) result;
    }
    
	/**
	 * Drops an existing column.
	 */
	public void dropColumn(String columnName) {
		DbAttribute attribute = alterColumn(columnName).getAttribute();
		getDatabase().addOperation(factory().createDropColumnToDb(getEntity(), attribute));
		attribute.getEntity().removeAttribute(columnName);
		getColumns().remove(columnName);
	}
	
	/**
     * Removes an existing foreign key constraint.
     * @param sourceColumnName
     * @param destinationTable
     * @param destinationColumnName
     */
    public void dropForeignKey(String sourceColumnName, String destinationTable, String destinationColumnName) {
        MigrationRelationship relationship = new MigrationRelationship(this, sourceColumnName, destinationTable, destinationColumnName);
        MergerToken op = factory().createDropRelationshipToDb(getEntity(), relationship.getRelationship());
        getDatabase().addOperation(op);
    }
	
}
