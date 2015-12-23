package br.ufpe.cin.util.db;

import java.io.File;
import java.sql.Connection;
import java.sql.DriverManager;

import javax.sql.PooledConnection;

import org.apache.commons.pool2.BasePooledObjectFactory;
import org.apache.commons.pool2.PooledObject;
import org.apache.commons.pool2.impl.DefaultPooledObject;
import org.apache.commons.pool2.impl.GenericObjectPool;
import org.apache.commons.pool2.impl.GenericObjectPoolConfig;
import org.hsqldb.jdbc.JDBCConnection;
import org.hsqldb.jdbc.pool.JDBCPooledConnection;

public class PooledHSQLDBConnectionFactory extends BasePooledObjectFactory<Connection> {
	
	private final String dbConnectionString;
	private final String dbUser;
	private final String dbPass;
	
	public PooledHSQLDBConnectionFactory(String connectionString, String dbUser, String dbPass) {
		dbConnectionString = connectionString;
		this.dbPass = dbPass;
		this.dbUser = dbUser;
	}
	
	public PooledHSQLDBConnectionFactory(File fileDB, String dbUser, String dbPass) {
		dbConnectionString = "jdbc:hsqldb:file:" + fileDB.getPath() + ";shutdown=true";
		this.dbPass = dbPass;
		this.dbUser = dbUser;
	}
	
	public static GenericObjectPool<Connection> newHSQLDBPool(File fileDB, String dbUser, String dbPass) {
		GenericObjectPoolConfig poolConfig = new GenericObjectPoolConfig();
		poolConfig.setBlockWhenExhausted(true);
		final int numProcs = Runtime.getRuntime().availableProcessors();
		poolConfig.setMaxIdle(numProcs);
		poolConfig.setMaxTotal(numProcs);
		GenericObjectPool<Connection> ret = new GenericObjectPool<Connection>(new PooledHSQLDBConnectionFactory(fileDB, dbUser, dbPass), poolConfig);
		return ret;		
	}
	

	@Override
	public Connection create() throws Exception {
		JDBCConnection conn = (JDBCConnection) DriverManager.getConnection(dbConnectionString, dbUser, dbPass);
		PooledConnection pooledConn = new JDBCPooledConnection(conn);
		return pooledConn.getConnection();
	}

	@Override
	public PooledObject<Connection> wrap(Connection conn) {
		return new DefaultPooledObject<Connection>(conn);
	}
	
    @Override
    public void destroyObject(PooledObject<Connection> pConn)
        throws Exception  {
    	JDBCConnection conn = (JDBCConnection) pConn.getObject();
    	conn.closeFully();
    }
	

}
