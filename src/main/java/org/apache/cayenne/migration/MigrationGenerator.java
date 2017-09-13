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

import java.io.File;
import java.io.FileWriter;
import java.io.Writer;
import java.sql.Types;

import org.apache.cayenne.configuration.server.ServerRuntime;
import org.apache.cayenne.configuration.server.ServerRuntimeBuilder;
import org.apache.cayenne.dba.TypesMapping;
import org.apache.cayenne.dbsync.DbSyncModule;
import org.apache.cayenne.map.DataMap;
import org.apache.cayenne.map.DbAttribute;
import org.apache.cayenne.map.DbEntity;
import org.apache.cayenne.map.DbRelationship;
import org.apache.commons.lang.StringUtils;

/**
 * Given a Cayenne project file (model) this will generate a Migration subclass that
 * includes all the operations to create a database from scratch for the first time.
 * 
 * @author john
 *
 */
public class MigrationGenerator {

	/**
	 * Runs the generator. Expects 2 arguments:<br>
	 * 1) The name or path to the cayenne project file (for example, "cayenne-MyDomain.xml")<br>
	 * 2) The output path for the generated java source files.
	 * 
	 * @param args
	 */
	public static void main(String[] args) {
		if (args.length != 2) {
			System.out.println("Usage:");
			System.out.println("java " + MigrationGenerator.class.getName() + " <cayenne-Project.xml> <output-folder>");
			return;
		}
		
		MigrationGenerator gen = new MigrationGenerator();
		gen.generateInitialMigration(args[0], args[1]);
	}
	
	private StringBuilder buffer;
	private ServerRuntime runtime;
	
	/**
	 * Generates the migration code necessary to recreate the entire database as defined by the cayenne project (model).
	 * 
	 * @param configurationLocation
	 * @param outputFolder
	 */
	public void generateInitialMigration(String configurationLocation, String outputFolder) {
	    ServerRuntimeBuilder builder = ServerRuntime.builder();
	    builder.addConfig(configurationLocation);
	    builder.addModule(new DbSyncModule());
		runtime = builder.build();
		ServerRuntime.bindThreadInjector( runtime.getInjector() );

		for (DataMap map : runtime.getDataDomain().getDataMaps()) {
			generateInitialMigration(map, outputFolder + "/" + className(map) + ".java");
		};
	}

	private String className(DataMap map) {
		return StringUtils.capitalize(map.getName()) + "0";
	}

	protected void generateInitialMigration(DataMap map, String outputFilename) {
		buffer = new StringBuilder();
		buffer.append(
				"import org.apache.cayenne.access.DataNode;\n" +
				"\n" +
				"import org.apache.cayenne.migration.Migration;\n" +
				"import org.apache.cayenne.migration.MigrationDatabase;\n" +
				"import org.apache.cayenne.migration.MigrationTableNew;\n" +
				"\n" +
				"public class " + className(map) + " extends Migration {\n" +
				"\n" +
				"\tpublic " + className(map) + "(DataNode node) {\n" +
				"\t\tsuper(node);\n" +
				"\t}\n" +
				"\n" +
				"\tpublic void upgrade(MigrationDatabase db) {\n" +
				"");
		
		for (DbEntity entity : map.getDbEntities()) {
			createTable(entity);
		}
		
		buffer.append("\n");
		
		for (DbEntity entity : map.getDbEntities()) {
			createForeignKeysForTable(entity);
		}
		
		buffer.append("\t}\n" +
				"\n" +
				"}");
		
		try {
			new File(outputFilename).getParentFile().mkdirs();
			
			Writer writer = new FileWriter(outputFilename);
			writer.write(buffer.toString());
			writer.flush();
			writer.close();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	protected String fullyQualifiedTableName(DbEntity entity) {
	    String fullyQualifiedTableName = StringUtils.isEmpty(entity.getCatalog()) ? "" : (entity.getCatalog()+".");
	    fullyQualifiedTableName += StringUtils.isEmpty(entity.getSchema()) ? "" : (entity.getSchema()+".");
	    fullyQualifiedTableName += entity.getName();
	    return fullyQualifiedTableName;
	}
	
	protected void createTable(DbEntity entity) {
		String tableName = tableName(entity);
		buffer.append("\t\tMigrationTableNew " + tableName + " = db.createTable(\"" + fullyQualifiedTableName(entity) + "\");\n");
		
		for (DbAttribute attribute : entity.getAttributes()) {
			String type = nameForJdbcType(attribute.getType());
			type = StringUtils.capitalize(type);
			
			buffer.append("\t\t");
			
			if (type == null) {
				// fixed point
				if (attribute.getScale() >= 0) {
					buffer.append(String.format("%s.addColumn(\"%s\", %d, %d, %d", tableName, attribute.getName(), attribute.getType(), attribute.getMaxLength(), attribute.getScale()));
				
				// character
				} else if (attribute.getMaxLength() >= 0) {
					buffer.append(String.format("%s.addColumn(\"%s\", %d, %d", tableName, attribute.getName(), attribute.getType(), attribute.getMaxLength()));
				
				// other
				} else {
					buffer.append(String.format("%s.addColumn(\"%s\", %d", tableName, attribute.getName(), attribute.getType()));
				}
			} else {
				// fixed point
				if (isFixedPoint(attribute.getType()) && attribute.getScale() >= 0) {
					buffer.append(String.format("%s.add%sColumn(\"%s\", %d, %d", tableName, type, attribute.getName(), attribute.getMaxLength(), attribute.getScale()));
				
				// character
				} else if (hasLength(attribute.getType()) && attribute.getMaxLength() >= 0) {
					buffer.append(String.format("%s.add%sColumn(\"%s\", %d", tableName, type, attribute.getName(), attribute.getMaxLength()));
				
				// other
				} else {
					buffer.append(String.format("%s.add%sColumn(\"%s\"", tableName, type, attribute.getName()));
				}
			}
			
			if (attribute.isMandatory()) {
				buffer.append(", MANDATORY, null");
			}
		
			buffer.append(");\n");
		}
		
		for (DbAttribute attribute : entity.getPrimaryKeys()) {
			buffer.append(String.format("\t\t%s.addPrimaryKey(\"%s\");\n", tableName, attribute.getName()));
		}
		
		buffer.append("\n");
	}
	
	protected void createForeignKeysForTable(DbEntity entity) {
		String tableName = tableName(entity);

		for (DbRelationship relationship : entity.getRelationships()) {
			if (!relationship.isToMany() && relationship.isToPK()) {
				buffer.append(String.format("\t\t%s.addForeignKey(\"%s\", \"%s\", \"%s\", true);\n", tableName,
						relationship.getSourceAttributes().iterator().next().getName(),
						fullyQualifiedTableName(relationship.getTargetEntity()),
						relationship.getTargetAttributes().iterator().next().getName()));
			}
		}
	}

	protected String tableName(DbEntity entity) {
		return StringUtils.uncapitalize(entity.getName());
	}

	protected String nameForJdbcType(int type) {
		switch (type) {
		case Types.ARRAY: return "array";
		case Types.BIGINT: return "bigInt";
		case Types.BINARY: return "binary";
		case Types.BIT: return "bit";
		case Types.BLOB: return "blob";
		case Types.BOOLEAN: return "boolean";
		case Types.CHAR: return "char";
		case Types.CLOB: return "clob";
		case Types.DATE: return "date";
		case Types.DECIMAL: return "decimal";
		case Types.DOUBLE: return "double";
		case Types.FLOAT: return "float";
		case Types.INTEGER: return "integer";
		case Types.LONGVARBINARY: return "longVarBinary";
		case Types.LONGVARCHAR: return "longVarChar";
		case Types.NUMERIC: return "numeric";
		case Types.REAL: return "real";
		case Types.SMALLINT: return  "smallInt";
		case Types.TIME: return "time";
		case Types.TIMESTAMP: return "timestamp";
		case Types.TINYINT: return "tinyInt";
		case Types.VARBINARY: return "varBinary";
		case Types.VARCHAR: return "varchar";
		default:
			return null;
		}
	}
	
	protected boolean isFixedPoint(int type) {
		return type == Types.DECIMAL || type == Types.NUMERIC;
	}
	
	protected boolean hasLength(int type) {
		return TypesMapping.supportsLength(type)
		        || type == Types.BLOB // for Derby
		        || type == Types.CLOB // for Derby
		        || type == Types.TIMESTAMP // for MySQL
		        || type == Types.TIME; // for MySQL
	}
	
}
