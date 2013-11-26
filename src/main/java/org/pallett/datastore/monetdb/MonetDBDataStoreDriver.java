package org.pallett.datastore.monetdb;

import java.sql.Connection;
import java.sql.Driver;
import java.sql.DriverManager;

import com.vividsolutions.jump.datastore.AdhocQuery;
import com.vividsolutions.jump.datastore.DataStoreConnection;
import com.vividsolutions.jump.datastore.DataStoreDriver;
import com.vividsolutions.jump.datastore.Query;
import com.vividsolutions.jump.feature.Feature;
import com.vividsolutions.jump.io.FeatureInputStream;
import com.vividsolutions.jump.parameter.ParameterList;
import com.vividsolutions.jump.parameter.ParameterListSchema;

public class MonetDBDataStoreDriver implements DataStoreDriver {
	
	public static final String DRIVER_NAME = "MonetDB";
	
	public static final String JDBC_CLASS = "nl.cwi.monetdb.jdbc.MonetDriver";
	
	public static final String URL_PREFIX = "jdbc:monetdb://";
	
	public static final String PARAM_Server = "Server";
	public static final String PARAM_Port = "Port";
	public static final String PARAM_Database = "Database";
	public static final String PARAM_User = "User";
	public static final String PARAM_Password = "Password";
	
	private static final String[] paramNames = new String[] {
		PARAM_Server,
		PARAM_Port,
		PARAM_Database,
		PARAM_User,
		PARAM_Password
	};
	
	@SuppressWarnings("rawtypes")
	private static final Class[] paramClasses = new Class[]{
		String.class,
		Integer.class,
		String.class,
		String.class,
		String.class
	};
	
	private final ParameterListSchema schema = new ParameterListSchema(paramNames, paramClasses);

	@Override
	public DataStoreConnection createConnection(ParameterList params) throws Exception {
		// get parameters
		String host = params.getParameterString(PARAM_Server);
		int port = params.getParameterInt(PARAM_Port);
		String database = params.getParameterString(PARAM_Database);
		String user = params.getParameterString(PARAM_User);
		String password = params.getParameterString(PARAM_Password);
		
		if (port == 0) port = 50000;
		
		String url = URL_PREFIX + host + ":" + port + "/" + database;

		Driver driver = (Driver) Class.forName(JDBC_CLASS).newInstance();
		DriverManager.registerDriver(driver);

		Connection conn = DriverManager.getConnection(url, user, password);
		return new MonetDBDataStoreConnection(conn);
	}

	@Override
	public boolean isAdHocQuerySupported() {
		return true;
	}

	@Override
	public ParameterListSchema getParameterListSchema() {
	    return schema;
	}

	@Override
	public String getName() {
	    return DRIVER_NAME;
	}
	
	
	public static void main (String[] args) throws Exception {
		DataStoreDriver driver = new MonetDBDataStoreDriver();
		
		ParameterList params = new ParameterList(new ParameterListSchema(paramNames, paramClasses));
		
		params.setParameter(PARAM_Database, "test");
		params.setParameter(PARAM_Server, "localhost");
		params.setParameter(PARAM_Port, 50000);
		params.setParameter(PARAM_User, "monetdb");
		params.setParameter(PARAM_Password, "monetdb");
		
		System.out.println("Setting up connection...");
		DataStoreConnection conn = driver.createConnection(params);
		System.out.println("Connection setup!");
		
		Query query = new AdhocQuery("SELECT gid, the_geom FROM nyc_buildings LIMIT 10");
		
		FeatureInputStream stream = conn.execute(query);
		
		while(stream.hasNext()) {
			Feature feature = stream.next();
			
			System.out.println(feature.getGeometry().getArea());
		}
		
		System.out.println("Closing connection...");
		conn.close();
		System.out.println("Connection closed!");
	}
	
	
}
