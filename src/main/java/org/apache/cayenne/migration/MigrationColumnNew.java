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

import java.util.List;

import org.apache.cayenne.dba.postgres.PostgresAdapter;
import org.apache.cayenne.merge.AbstractToDbToken;
import org.apache.cayenne.merge.ArbitrarySqlToDb;
import org.apache.cayenne.merge.MergerToken;

/**
 * Represents a new column in the database.
 * 
 * Internally this holds a DbAttribute and uses it along with a MergerFactory to queue up
 * MergerTokens to perform the schema changes.
 * 
 * @author john
 *
 */
public class MigrationColumnNew extends MigrationColumn {

	MigrationColumnNew(MigrationTable table, String name, int jdbcType, int maxLength, int precision, int scale, boolean isMandatory, Object defaultValue) {
		super(table, name, jdbcType, maxLength, precision, scale, isMandatory, defaultValue);
		if (!table.isNew()) {
			create();
		} else if (getDefaultValue() != null) {
            setDefault(getDefaultValue());
        }
	}

	@Override
	boolean isNew() {
		return true;
	}
	
	protected void create() {
	    if (Migrator.USE_EFFICIENT_ALTER_TABLE) {
	           MergerToken op = factory().createAddColumnToDb(getTable().getEntity(), getAttribute());
	           String sql = ((AbstractToDbToken)op).createSql(getTable().getDatabase().getAdapter()).get(0);
	           
	           if (getDefaultValue() != null) {
	               sql += " DEFAULT " + sqlForLiteral(getDefaultValue());
	           }
	           
	           if (getAttribute().isMandatory()) {
	               sql += " NOT NULL";
	           }
	           
	           getTable().getDatabase().addOperation(new ArbitrarySqlToDb(sql));
	    } else {
    		MergerToken op = factory().createAddColumnToDb(getTable().getEntity(), getAttribute());
    		getTable().getDatabase().addOperation(op);
    		
    		if (getDefaultValue() != null) {
                setDefault(getDefaultValue());
            }
    		
    		if (getAttribute().isMandatory()) {
    		    getTable().getDatabase().execute("UPDATE " +  getTable().getEntity().getFullyQualifiedName() + " SET " + getAttribute().getName() + " = " + sqlForLiteral(getDefaultValue()));
    		    
                op = factory().createSetNotNullToDb(getTable().getEntity(), getAttribute());
                getTable().getDatabase().addOperation(op);
    		}
	    }
	}
	
}
