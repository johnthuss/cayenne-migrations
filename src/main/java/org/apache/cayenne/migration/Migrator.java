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
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;

import org.apache.cayenne.access.DataNode;
import org.apache.cayenne.dbsync.merge.token.MergerToken;
import org.apache.cayenne.dbsync.merge.token.db.AbstractToDbToken;
import org.apache.cayenne.log.JdbcEventLogger;
import org.apache.cayenne.map.DataMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The Migrator discovers and executes Migration subclasses in order to migrate
 * a database schema to the latest version.
 * 
 * @author john
 *
 */
public class Migrator {

    private static final Logger log = LoggerFactory.getLogger(Migrator.class);

    public static boolean USE_EFFICIENT_ALTER_TABLE = false;
    
	private final DataNode node;
	private final String migrationsPackage;
	private Connection connection;
	
	/**
	 * 
	 * @param node the node that the schema migrations should be applied to
	 * @param migrationsPackage the package that your migration subclasses reside in
	 */
	public Migrator(DataNode node, String migrationsPackage) {
		this.node = node;
		this.migrationsPackage = migrationsPackage;
	}

	/**
	 * The name of the table that holds the version and lock information for the Migrator to use.
	 * @return
	 */
	protected String migrationTableName() {
		return "dbupdater";
	}
	
	void createInternalMigrationSchema() throws SQLException {
		executeSqlWithUpdateCount("CREATE TABLE " + migrationTableName() + "(dataMap VARCHAR(50) NOT NULL, version INTEGER DEFAULT -1 NOT NULL, locked SMALLINT DEFAULT 0 NOT NULL, PRIMARY KEY(dataMap))");
		getConnection().commit();
	}
	
	int currentDbVersion(DataMap map) throws SQLException {
	    String sql = String.format("SELECT version FROM %s WHERE dataMap = '%s'", migrationTableName(), map.getName());
	    Integer version = null;
	    try {
	        version = executeSqlReturnInt(sql);
	        return version != null ? version.intValue() : -1;
	    } catch (Exception e) {
	        try {
	            createInternalMigrationSchema();
	        } catch (Exception e2) {
	            node.getJdbcEventLogger().log(e.getMessage());
	        }
	        version = executeSqlReturnInt(sql);
	        return version != null ? version.intValue() : -1;
	    } finally {
	        getConnection().commit();
	    }
	}
	
	void setDbVersion(DataMap map, int version) throws SQLException {
		int count = executeSqlWithUpdateCount(String.format("UPDATE %s SET version = %d WHERE version = %d AND dataMap = '%s'", migrationTableName(), version, version-1, map.getName()));
		if (count == 0) {
			throw new RuntimeException("Unable to update database version for dataMap: " + map.getName());
		}
	}
	
	boolean lock(DataMap map) throws SQLException {
		String sql = String.format("UPDATE %s SET locked = 1 WHERE locked=0 and dataMap='%s'", migrationTableName(), map.getName());
		int count = 0;
		try {
			count = executeSqlWithUpdateCount(sql);
			if (count > 0) {
				return true; // got the lock
			}
		
	        sql = String.format("SELECT locked FROM %s WHERE dataMap='%s'", migrationTableName(), map.getName());
	        Integer locked = executeSqlReturnInt(sql);
	        if (locked != null) {
	        	return false; // row exists and is already locked
	        } else {
	        	// row doesn't exist
	        	sql = String.format("INSERT INTO %s(dataMap, locked) VALUES ('%s', 1)", migrationTableName(), map.getName());
	        	executeSqlWithUpdateCount(sql);
	        	return true;
	        }
		} finally {
		    getConnection().commit();
		}
	}
	
	String unlockSql(DataMap map) {
	    return String.format("UPDATE %s SET locked = 0 WHERE locked = 1 AND dataMap = '%s'", migrationTableName(), map.getName());
	}
	
	void unlock(DataMap map) throws SQLException {
		int count = executeSqlWithUpdateCount(unlockSql(map));
		if (count == 0) {
			throw new IllegalStateException("Unable to remove migration lock.");
		}
		getConnection().commit();
	}
	
	Migration createMigrationClassForVersion(DataMap map, int version) {
		String className = migrationsPackage + "." + MigrationGenerator.capitalize(map.getName()) + version;
		
		Class<?> clazz;
		try {
			clazz = Class.forName(className);
			Migration instance = (Migration) clazz.getConstructor(DataNode.class).newInstance(node);
			return instance;
		} catch (Exception e) {
			log.debug("Migration class not found: " + className + "; stopping at version " + (version-1) + ".");
			return null;
		}
	}
	
	/**
	 * Discovers and executes the Migrations necessary to update the database schema to the latest version.
	 * 
	 * @throws SQLException
	 */
	public void migrateToLatest() throws SQLException {
		synchronized (node) {
            try {
				getConnection();
	            
				for (DataMap map : node.getDataMaps()) {
					
                    int version = currentDbVersion(map)+1;

                    Migration migration = createMigrationClassForVersion(map, version);
                    if (migration != null) {
                        while (!lock(map)) {
                            log.warn("Waiting to obtain migration lock for node: " + node.getName() + ". " +
                                    "If you terminated the application while a migration was in progress " +
                                    "you will need to clear the migration lock by running: " +
                                    unlockSql(map));
                            try {
                                Thread.sleep(2000);
                            } catch (InterruptedException e) {
                                return;
                            }
                        }
					
                        version = currentDbVersion(map)+1;
                        
    					try {
    						while ((migration = createMigrationClassForVersion(map, version)) != null) {
    						    log.info(String.format("Updating dataMap '%s' to version %d", map.getName(), version));
    						    migration.getDatabase().setDatabaseProductName(getConnection().getMetaData().getDatabaseProductName());
    							migration.run();
    							try {
    							    executeOperations(migration.getDatabase().getOperations());
    							} catch (Exception e) {
    							    throw new RuntimeException("Failed to migrate node=" + node.getName() + ", dataMap=" + map.getName() + " to version=" + version + ": " + e.getMessage());
    							}
    							setDbVersion(map, version);
    							getConnection().commit();
    							version++;
    						}
    					} finally {
    						unlock(map);
    					}
                    }
				}
				
			} finally {
	            if (getConnection() != null) {
	                try {
	                    getConnection().close();
	                } catch (SQLException e) {}
	            }
	        }
		}
	}

	void executeOperations(List<MergerToken> operations) throws SQLException {
		for (MergerToken token : operations) {
			AbstractToDbToken dbToken = (AbstractToDbToken)token;
			executeSqlWithUpdateCount(dbToken.createSql(node.getAdapter()));
		}
	}
	
	void executeSqlWithUpdateCount(List<String> sqlStatements) throws SQLException {
    	for (String sql : sqlStatements) {
			executeSqlWithUpdateCount(sql);
		}
    }
    
	int executeSqlWithUpdateCount(String sql) throws SQLException {
        Statement st = null;
        JdbcEventLogger logger = node.getJdbcEventLogger();
        try {
            logger.log(sql);
            st = getConnection().createStatement();
            try {
            	st.execute(sql);
            } catch (SQLException e) {
                getConnection().rollback();
            	throw new RuntimeException("SQL statement failed \"" + sql + "\": " + e.getMessage(), e);
            }
            return st.getUpdateCount();
        } finally {
            closeStatement(st);
        }
    }
    
	Integer executeSqlReturnInt(String sql) throws SQLException {
        Statement st = null;
        JdbcEventLogger logger = node.getJdbcEventLogger();
        try {
            logger.log(sql);
            st = getConnection().createStatement();
            try {
                st.execute(sql);
                ResultSet rs = st.getResultSet();
                if (rs != null && rs.next()) {
                    return rs.getInt(1);
                } else {
                	return null;
                }
            } catch (SQLException e) {
                getConnection().rollback();
                throw new RuntimeException("SQL statement failed \"" + sql + "\": " + e.getMessage(), e);
            }
        } finally {
            closeStatement(st);
        }
	}

	private void closeStatement(Statement st) {
		if (st != null) {
		    try {
		        st.close();
		    } catch (SQLException e) {}
		}
	}

    Connection getConnection() throws SQLException {
        if (connection == null) {
            connection = node.getDataSource().getConnection();
            getConnection().setAutoCommit(false);
        }
        return connection;
    }
	
}
