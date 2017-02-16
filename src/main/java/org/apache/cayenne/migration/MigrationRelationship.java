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

import org.apache.cayenne.dbsync.merge.factory.MergerTokenFactory;
import org.apache.cayenne.map.DbAttribute;
import org.apache.cayenne.map.DbEntity;
import org.apache.cayenne.map.DbJoin;
import org.apache.cayenne.map.DbRelationship;

/**
 * Represents a foreign key constraint in the database and provides operations for changing the schema.
 * 
 * Internally this holds a DbRelationship and uses it along with a MergerFactory to queue up
 * MergerTokens to perform the schema changes.
 * 
 * @author john
 *
 */
class MigrationRelationship {

	private final MigrationTable table;
	private final DbRelationship relationship;
	
	MigrationRelationship(MigrationTable table, String sourceColumnName, String destinationTable, String destinationColumnName) {
		this.table = table;
		
		if (table.getEntity().getAttribute(sourceColumnName) == null) {
	          DbAttribute sourceAttribute = new DbAttribute(sourceColumnName);
	          table.getEntity().addAttribute(sourceAttribute);
	          sourceAttribute.setEntity(table.getEntity());
		}

		DbEntity targetEntity = table.getDatabase().getDataMap().getDbEntity(destinationTable);
		if (targetEntity == null) {
		    for (DbEntity entity : table.getDatabase().getDataMap().getDbEntities()) {
		        if (entity.getName().equalsIgnoreCase(destinationTable)) {
		            targetEntity = entity;
		            break;
		        }
		    }
		    
		    if (targetEntity == null) {
    			targetEntity = new DbEntity(destinationTable);
    			table.getDatabase().getDataMap().addDbEntity(targetEntity);
    			targetEntity.setDataMap(table.getDatabase().getDataMap());
		    }
		}
		
		DbAttribute targetAttribute = targetEntity.getAttribute(destinationColumnName);
		if (targetAttribute == null) {
		    for (DbAttribute attr : targetEntity.getAttributes()) {
		        if (attr.getName().equalsIgnoreCase(destinationColumnName)) {
		            targetAttribute = attr;
		            break;
		        }
		    }

		    if (targetAttribute == null) {
                targetAttribute = new DbAttribute(destinationColumnName);
                targetEntity.addAttribute(targetAttribute);
                targetAttribute.setEntity(targetEntity);
		    }
		}
		
		if (!targetAttribute.isPrimaryKey()) {
            targetAttribute.setPrimaryKey(true);
		}
		
        relationship = new DbRelationship("relationship" + table.getEntity().getRelationships().size());
        relationship.setSourceEntity(table.getEntity());
        table.getEntity().addRelationship(relationship);
        
        relationship.setTargetEntityName(destinationTable);
        relationship.addJoin(new DbJoin(relationship, sourceColumnName, destinationColumnName));
	}
	
	DbRelationship getRelationship() {
		return relationship;
	}
	
	MergerTokenFactory factory() {
		return table.factory();
	}
	
}
