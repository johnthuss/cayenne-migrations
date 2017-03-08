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

import java.math.BigDecimal;
import java.text.SimpleDateFormat;
import java.util.Collections;
import java.util.Date;
import java.util.List;

import org.apache.cayenne.dbsync.merge.factory.MergerTokenFactory;
import org.apache.cayenne.dbsync.merge.token.ValueForNullProvider;
import org.apache.cayenne.map.DbAttribute;
import org.apache.cayenne.map.DbEntity;

/**
 * Represents a column in the database and provides operations for changing the schema.
 * 
 * Internally this holds a DbAttribute and uses it along with a MergerFactory to queue up
 * MergerTokens to perform the schema changes.
 * 
 * @author john
 *
 */
public abstract class MigrationColumn {

	private final MigrationTable table;
	private final DbAttribute attribute;
	private Object defaultValue;
	
	MigrationColumn(MigrationTable table, String name, int jdbcType, int maxLength, int precision, int scale, boolean isMandatory, Object defaultValue) {
		this.table = table;

		if (this.table.getColumns().containsKey(name)) {
			throw new IllegalStateException(name + " has already been added to table: " + table.getEntity().getName());
		}
		this.table.getColumns().put(name, this);
		
		this.attribute = new DbAttribute(name);
		attribute.setEntity(table.getEntity());
		table.getEntity().addAttribute(attribute);
		
		attribute.setType(jdbcType);
		attribute.setMandatory(isMandatory);
		attribute.setScale(scale);
		if (precision > -1) {
		    attribute.setAttributePrecision(precision);
		    attribute.setMaxLength(precision);
		} else {
	        attribute.setMaxLength(maxLength);
		}
	    this.defaultValue = defaultValue;
	}

	MigrationTable getTable() {
	    return table;
	}
	
	DbAttribute getAttribute() {
		return attribute;
	}
	
	Object getDefaultValue() {
	    return defaultValue;
	}
	
	MergerTokenFactory factory() {
		return table.factory();
	}
	
	abstract boolean isNew();
	
	   /**
     * Changes the default value for a column. Pass NULL to remove the default.
     * @param value the new default or NULL for no default
     */
    protected MigrationColumn setDefault(final Object value) {
        getTable().getDatabase().addOperation(
                factory().createSetValueForNullToDb(
                        getTable().getEntity(),
                        getAttribute(),
                        new ValueForNullProvider() {

                            @Override
                            public boolean hasValueFor(DbEntity entity, DbAttribute column) {
                                return value != null;
                            }

                            @Override
                            public List<String> createSql(
                                    DbEntity entity,
                                    DbAttribute column) {
                                return Collections.singletonList(alterColumnDefaultValue(
                                        getAttribute(),
                                        value));
                            }
                        }));
        return this;
    }
    
    /**
     * @return an SQL statement that will add or remove a default value to/from a column
     */
    protected String alterColumnDefaultValue(DbAttribute column, Object defaultValue) {
        return String.format("ALTER TABLE %s ALTER %s SET DEFAULT %s",
                column.getEntity().getName(),
                column.getName(),
                sqlForLiteral(defaultValue));
    }
    
    /**
     * @return 'value' as a literal sql string
     */
    protected String sqlForLiteral(Object value) {
        if (value == null) {
            return "null";
        } else if (value instanceof String) {
            return "'" + value + "'";
        } else if (value instanceof BigDecimal) {
            return ((BigDecimal) value).toPlainString();
        } else if (value instanceof Number) {
            return value.toString();
        } else if (value instanceof Date) {
            SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
            return "'" + dateFormat.format(value) + "'";
        } else {
            return value.toString();
        }
    }
    
}

