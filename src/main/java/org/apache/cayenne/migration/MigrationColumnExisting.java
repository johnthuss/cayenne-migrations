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



/**
 * Represents an existing column in the database and provides operations for changing the schema.
 * 
 * Internally this holds a DbAttribute and uses it along with a MergerFactory to queue up
 * MergerTokens to perform the schema changes.
 * 
 * @author john
 *
 */
public class MigrationColumnExisting extends MigrationColumn {

	MigrationColumnExisting(MigrationTable table, String name) {
		super(table, name, Types.OTHER, 0, 0, 0, false, null);
	}

	@Override
	boolean isNew() {
		return false;
	}
	
	/**
	 * Adds a "NOT NULL" constraint to an existing column.
	 */
	public MigrationColumnExisting addNotNullConstraint() {
		getAttribute().setMandatory(true);
		getTable().getDatabase().addOperation(factory().createSetNotNullToDb(getTable().getEntity(), getAttribute()));
		return this;
	}

	/**
	 * Removes a "NOT NULL" constraint from an existing column.
	 */
	public MigrationColumnExisting dropNotNullConstraint() {
	    getAttribute().setMandatory(false);
	    getTable().getDatabase().addOperation(factory().createSetAllowNullToDb(getTable().getEntity(), getAttribute()));
		return this;
	}
	
	/**
	 * Changes the data type for an existing column with configurable precision and scale.
	 * @param jdbcType
	 * @param precision
	 * @param scale
	 */
	public MigrationColumnExisting setDataType(int jdbcType, int precision, int scale) {
	    getAttribute().setType(jdbcType);
	    getAttribute().setAttributePrecision(precision);
	    getAttribute().setMaxLength(precision);
	    getAttribute().setScale(scale);
		getTable().getDatabase().addOperation(factory().createSetColumnTypeToDb(getTable().getEntity(), getAttribute(), getAttribute()));
		return this;
	}
	
	/**
	 * Changes the data type for an existing column with configure width
	 * @param jdbcType
	 * @param width
	 */
	public MigrationColumnExisting setDataType(int jdbcType, int width) {
	    getAttribute().setType(jdbcType);
	    getAttribute().setMaxLength(width);
	    getTable().getDatabase().addOperation(factory().createSetColumnTypeToDb(getTable().getEntity(), getAttribute(), getAttribute()));
		return this;
	}
	
	/**
	 * Changes the data type for an existing column
	 * @param jdbcType
	 */
	public MigrationColumnExisting setDataType(int jdbcType) {
	    getAttribute().setType(jdbcType);
	    getTable().getDatabase().addOperation(factory().createSetColumnTypeToDb(getTable().getEntity(), getAttribute(), getAttribute()));
		return this;
	}
	
}
