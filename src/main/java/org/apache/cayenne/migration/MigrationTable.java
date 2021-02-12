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
import java.util.HashMap;
import java.util.Map;

import org.apache.cayenne.dbsync.merge.factory.MergerTokenFactory;
import org.apache.cayenne.dbsync.merge.token.MergerToken;
import org.apache.cayenne.map.DbEntity;
import org.apache.cayenne.merge.ArbitrarySqlToDb;

/**
 * Represents a table in the database and provides operations for changing the schema.
 * 
 * Internally this holds a DbEntity and uses it along with a MergerFactory to queue up
 * MergerTokens to perform the schema changes.
 * 
 * @author john
 *
 */
public abstract class MigrationTable {

	private MigrationDatabase database;
	private DbEntity entity;
	private Map<String, MigrationColumn> columns = new HashMap<String, MigrationColumn>();

	MigrationTable(MigrationDatabase database, String tableName) {
		this.database = database;
		
		this.entity = new DbEntity(tableName);
		entity.setDataMap(database.getDataMap());
		database.getDataMap().addDbEntity(entity);
	}

	MigrationDatabase getDatabase() {
		return database;
	}
	
	MergerTokenFactory factory() {
		return database.factory();
	}
	
	DbEntity getEntity() {
		return entity;
	}
    
    Map<String, MigrationColumn> getColumns() {
        return columns;
    }
	
	abstract boolean isNew();
	
	/**
	 * Adds a column to a table; works for both new and existing tables.
	 * @param columnName
	 * @param jdbcType
	 * @return
	 */
	public MigrationColumnNew addColumn(String columnName, int jdbcType) {
		return addColumn(columnName, jdbcType, false, null);
	}
	public MigrationColumnNew addColumn(String columnName, int jdbcType, boolean isMandatory, Object defaultValue) {
		return new MigrationColumnNew(this, columnName, jdbcType, -1, -1, -1, isMandatory, defaultValue);
	}
	
	public MigrationColumnNew addColumn(String columnName, int jdbcType, int maxLength) {
		return addColumn(columnName, jdbcType, maxLength, false, null);
	}
	public MigrationColumnNew addColumn(String columnName, int jdbcType, int maxLength, boolean isMandatory, Object defaultValue) {
		return new MigrationColumnNew(this, columnName, jdbcType, maxLength, -1, -1, isMandatory, defaultValue);
	}
	
	public MigrationColumnNew addColumn(String columnName, int jdbcType, int precision, int scale) {
		return addColumn(columnName, jdbcType, precision, scale, false, null);
	}
	public MigrationColumnNew addColumn(String columnName, int jdbcType, int precision, int scale, boolean isMandatory, Object defaultValue) {
		return new MigrationColumnNew(this, columnName, jdbcType, -1, precision, scale, isMandatory, defaultValue);
	}
	
	public MigrationColumnNew addArrayColumn(String columnName) {
		return addArrayColumn(columnName, false);
	}
	public MigrationColumnNew addArrayColumn(String columnName, boolean isMandatory) {
		return addColumn(columnName, Types.ARRAY, isMandatory, null);
	}
	
	public MigrationColumnNew addBigIntColumn(String columnName) {
		return addBigIntColumn(columnName, false, null);
	}
	public MigrationColumnNew addBigIntColumn(String columnName, boolean isMandatory, Object defaultValue) {
		return addColumn(columnName, Types.BIGINT, isMandatory, defaultValue);
	}
	
	public MigrationColumnNew addBinaryColumn(String columnName, int maxLength) {
		return addBinaryColumn(columnName, maxLength, false, null);
	}
	public MigrationColumnNew addBinaryColumn(String columnName, int maxLength, boolean isMandatory, Object defaultValue) {
		return addColumn(columnName, Types.BINARY, maxLength, isMandatory, defaultValue);
	}
	
	public MigrationColumnNew addBitColumn(String columnName) {
		return addBitColumn(columnName, false, null);
	}
	public MigrationColumnNew addBitColumn(String columnName, boolean isMandatory, Object defaultValue) {
		return addColumn(columnName, Types.BIT, isMandatory, defaultValue);
	}
	
	public MigrationColumnNew addBlobColumn(String columnName) {
		return addBlobColumn(columnName, 0, false, null);
	}
	public MigrationColumnNew addBlobColumn(String columnName, int maxLength) {
	    return addBlobColumn(columnName, maxLength, false, null);
	}
	public MigrationColumnNew addBlobColumn(String columnName, boolean isMandatory, Object defaultValue) {
	    return addBlobColumn(columnName, 0, isMandatory, defaultValue);
	}
	public MigrationColumnNew addBlobColumn(String columnName, int maxLength, boolean isMandatory, Object defaultValue) {
		return addColumn(columnName, Types.BLOB, maxLength, isMandatory, defaultValue);
	}
	
	public MigrationColumnNew addCharColumn(String columnName, int maxLength) {
		return addCharColumn(columnName, maxLength, false, null);
	}
	public MigrationColumnNew addCharColumn(String columnName, int maxLength, boolean isMandatory, Object defaultValue) {
		return addColumn(columnName, Types.CHAR, maxLength, isMandatory, defaultValue);
	}
	
	public MigrationColumnNew addBooleanColumn(String columnName) {
		return addBooleanColumn(columnName, false, null);
	}
	public MigrationColumnNew addBooleanColumn(String columnName, boolean isMandatory, Object defaultValue) {
		return addColumn(columnName, Types.BOOLEAN, isMandatory, defaultValue);
	}
	
	public MigrationColumnNew addClobColumn(String columnName) {
		return addClobColumn(columnName, false, null);
	}
	public MigrationColumnNew addClobColumn(String columnName, int maxLength) {
	    return addClobColumn(columnName, maxLength, false, null);
	}
	public MigrationColumnNew addClobColumn(String columnName, boolean isMandatory, Object defaultValue) {
	    return addClobColumn(columnName, 0, false, null);
	}
	public MigrationColumnNew addClobColumn(String columnName, int maxLength, boolean isMandatory, Object defaultValue) {
		return addColumn(columnName, Types.CLOB, maxLength, isMandatory, defaultValue);
	}
	
	public MigrationColumnNew addDateColumn(String columnName) {
		return addDateColumn(columnName, false, null);
	}
	public MigrationColumnNew addDateColumn(String columnName, boolean isMandatory, Object defaultValue) {
		return addColumn(columnName, Types.DATE, isMandatory, defaultValue);
	}
	
	public MigrationColumnNew addDecimalColumn(String columnName, int precision, int scale) {
		return addDecimalColumn(columnName, precision, scale, false, null);
	}
	public MigrationColumnNew addDecimalColumn(String columnName, int precision, int scale, boolean isMandatory, Object defaultValue) {
		return addColumn(columnName, Types.DECIMAL, precision, scale, isMandatory, defaultValue);
	}

	public MigrationColumnNew addDoubleColumn(String columnName) {
		return addDoubleColumn(columnName, false, null);
	}
	public MigrationColumnNew addDoubleColumn(String columnName, boolean isMandatory, Object defaultValue) {
		return addColumn(columnName, Types.DOUBLE, isMandatory, defaultValue);
	}
	
	public MigrationColumnNew addFloatColumn(String columnName) {
		return addFloatColumn(columnName, false, null);
	}
	public MigrationColumnNew addFloatColumn(String columnName, boolean isMandatory, Object defaultValue) {
		return addColumn(columnName, Types.FLOAT, isMandatory, defaultValue);
	}
	
	public MigrationColumnNew addIntegerColumn(String columnName) {
		return addIntegerColumn(columnName, false, null);
	}
	public MigrationColumnNew addIntegerColumn(String columnName, boolean isMandatory, Object defaultValue) {
		return addColumn(columnName, Types.INTEGER, isMandatory, defaultValue);
	}

	public MigrationColumnNew addLongVarBinaryColumn(String columnName) {
		return addLongVarBinaryColumn(columnName, false);
	}
	public MigrationColumnNew addLongVarBinaryColumn(String columnName, boolean isMandatory) {
		return addColumn(columnName, Types.LONGVARBINARY, isMandatory, null);
	}
	
	public MigrationColumnNew addLongVarCharColumn(String columnName) {
		return addLongVarCharColumn(columnName, false, null);
	}
	public MigrationColumnNew addLongVarCharColumn(String columnName, boolean isMandatory, Object defaultValue) {
		return addColumn(columnName, Types.LONGVARCHAR, isMandatory, defaultValue);
	}
	
	public MigrationColumnNew addNumericColumn(String columnName, int precision, int scale) {
		return addNumericColumn(columnName, precision, scale, false, null);
	}
	public MigrationColumnNew addNumericColumn(String columnName, int precision, int scale, boolean isMandatory, Object defaultValue) {
		return addColumn(columnName, Types.NUMERIC, precision, scale, isMandatory, defaultValue);
	}
	
	public MigrationColumnNew addRealColumn(String columnName) {
		return addRealColumn(columnName, false, null);
	}
	public MigrationColumnNew addRealColumn(String columnName, boolean isMandatory, Object defaultValue) {
		return addColumn(columnName, Types.REAL, isMandatory, defaultValue);
	}
	
	public MigrationColumnNew addSmallIntColumn(String columnName) {
		return addSmallIntColumn(columnName, false, null);
	}
	public MigrationColumnNew addSmallIntColumn(String columnName, boolean isMandatory, Object defaultValue) {
		return addColumn(columnName, Types.SMALLINT, isMandatory, defaultValue);
	}
	
	public MigrationColumnNew addTimeColumn(String columnName) {
		return addTimeColumn(columnName, 0, false, null);
	}
    public MigrationColumnNew addTimeColumn(String columnName, boolean isMandatory, Object defaultValue) {
        return addTimeColumn(columnName, 0, isMandatory, defaultValue);
    }
    public MigrationColumnNew addTimeColumn(String columnName, int maxLength) {
        return addTimeColumn(columnName, maxLength, false, null);
    }
	public MigrationColumnNew addTimeColumn(String columnName, int maxLength, boolean isMandatory, Object defaultValue) {
		return addColumn(columnName, Types.TIME, maxLength, isMandatory, defaultValue);
	}
	
	public MigrationColumnNew addTimestampColumn(String columnName) {
		return addTimestampColumn(columnName, 0, false, null);
	}
    public MigrationColumnNew addTimestampColumn(String columnName, boolean isMandatory, Object defaultValue) {
        return addTimestampColumn(columnName, 0, isMandatory, defaultValue);
    }
    public MigrationColumnNew addTimestampColumn(String columnName, int maxLength) {
        return addTimestampColumn(columnName, maxLength, false, null);
    }
	public MigrationColumnNew addTimestampColumn(String columnName, int maxLength, boolean isMandatory, Object defaultValue) {
		return addColumn(columnName, Types.TIMESTAMP, maxLength, isMandatory, defaultValue);
	}
	
	public MigrationColumnNew addTinyIntColumn(String columnName) {
		return addTinyIntColumn(columnName, false, null);
	}
	public MigrationColumnNew addTinyIntColumn(String columnName, boolean isMandatory, Object defaultValue) {
		return addColumn(columnName, Types.TINYINT, isMandatory, defaultValue);
	}
	
	public MigrationColumnNew addVarBinaryColumn(String columnName, int maxLength) {
		return addVarBinaryColumn(columnName, maxLength, false);
	}
	public MigrationColumnNew addVarBinaryColumn(String columnName, int maxLength, boolean isMandatory) {
		return addColumn(columnName, Types.VARBINARY, maxLength, isMandatory, null);
	}
	
	public MigrationColumnNew addVarcharColumn(String columnName, int maxLength) {
		return addVarcharColumn(columnName, maxLength, false, null);
	}
	public MigrationColumnNew addVarcharColumn(String columnName, int maxLength, boolean isMandatory, Object defaultValue) {
		return addColumn(columnName, Types.VARCHAR, maxLength, isMandatory, defaultValue);
	}

	/**
	 * Adds a column to the list of primary keys for this table; works for both new and existing tables.
	 * @param columnName
	 */
	public void addPrimaryKey(String columnName) {
		entity.getAttribute(columnName).setPrimaryKey(true);
		if (!isNew()) {
			MigrationColumn column = columns.get(columnName);
			MergerToken op = factory().createSetPrimaryKeyToDb(getEntity(), Collections.EMPTY_LIST, Collections.singletonList(column.getAttribute()), null);
			getDatabase().addOperation(op);
		}
	}

	/**
	 * Adds a new foreign key constraint; works for both new and existing tables.
	 * @param sourceColumnName
	 * @param destinationTable
	 * @param destinationColumnName
	 */
	public void addForeignKey(String sourceColumnName, String destinationTable, String destinationColumnName) {
		addForeignKey(sourceColumnName, destinationTable, destinationColumnName, false);
	}
	
    /**
     * Adds a new foreign key constraint; works for both new and existing tables.
     * @param sourceColumnName
     * @param destinationTable
     * @param destinationColumnName
     */
    public void addForeignKey(String sourceColumnName, String destinationTable, String destinationColumnName, boolean shouldCreateIndexIfNeeded) {
        if (shouldCreateIndexIfNeeded) {
            String indexName = getEntity().getName().replaceFirst(".+\\.", "") + "_" + sourceColumnName + "_idx";
            // TODO: the specific SQL should really come from the DbAdapter
            getDatabase().addOperation(new ArbitrarySqlToDb(String.format("CREATE INDEX %s ON %s(%s)", indexName, getEntity().getName(), sourceColumnName)));
        }
        
        MigrationRelationship relationship = new MigrationRelationship(this, sourceColumnName, destinationTable, destinationColumnName);
        MergerToken op = factory().createAddRelationshipToDb(getEntity(), relationship.getRelationship());
        getDatabase().addOperation(op);
    }
}
