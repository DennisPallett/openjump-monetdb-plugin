/**
* The MIT License (MIT)
* 
* Copyright (c) 2013 Dennis Pallett
* 
* Permission is hereby granted, free of charge, to any person obtaining a copy of
* this software and associated documentation files (the "Software"), to deal in
* the Software without restriction, including without limitation the rights to
* use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of
* the Software, and to permit persons to whom the Software is furnished to do so,
* subject to the following conditions:
* 
* The above copyright notice and this permission notice shall be included in all
* copies or substantial portions of the Software.

* THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
* IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS
* FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR
* COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER
* IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN
* CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
*/
package org.pallett.datastore.monetdb;

import java.sql.Connection;
import java.sql.SQLException;

import com.vividsolutions.jump.datastore.AdhocQuery;
import com.vividsolutions.jump.datastore.DataStoreConnection;
import com.vividsolutions.jump.datastore.DataStoreException;
import com.vividsolutions.jump.datastore.DataStoreMetadata;
import com.vividsolutions.jump.datastore.FilterQuery;
import com.vividsolutions.jump.datastore.Query;
import com.vividsolutions.jump.datastore.SpatialReferenceSystemID;
import com.vividsolutions.jump.io.FeatureInputStream;

public class MonetDBDataStoreConnection implements DataStoreConnection {

	private Connection connection;
	
	private MonetDBDataStoreMetadata dbMetadata;
	
	public MonetDBDataStoreConnection(Connection conn) {
		connection = conn;

		dbMetadata = new MonetDBDataStoreMetadata(this);
	}
	
	public Connection getConnection() {
        return connection;
    }
	
	@Override
	public DataStoreMetadata getMetadata() {
		return dbMetadata;
	}

	@Override
	public FeatureInputStream execute(Query query) throws Exception {
		if (query instanceof FilterQuery) {
			 try {
				 return executeFilterQuery((FilterQuery) query);
			 } catch (SQLException e) {
				 throw new RuntimeException(e);
			 }
		}
		if (query instanceof AdhocQuery) {
			return executeAdhocQuery((AdhocQuery) query);
		}
		throw new IllegalArgumentException("Unsupported Query type");
	}
	
	/**
	 * Executes a filter query.
	 *
	 * The SRID is optional for queries - it will be determined automatically
	 * from the table metadata if not supplied.
	 *
	 * @param query the query to execute
	 * @return the results of the query
	 * @throws SQLException
	 */
	public FeatureInputStream executeFilterQuery(FilterQuery query) throws SQLException
	{	
		SpatialReferenceSystemID srid = dbMetadata.getSRID(query.getDatasetName(), query.getGeometryAttributeName());
		String[] colNames = dbMetadata.getColumnNames(query.getDatasetName());

		MonetDBSQLBuilder builder = new MonetDBSQLBuilder(srid, colNames);
		String queryString = builder.getSQL(query);

		MonetDBFeatureInputStream ifs = new MonetDBFeatureInputStream(connection, queryString);
		return ifs;
	}

	public FeatureInputStream executeAdhocQuery(AdhocQuery query)
	{
		String queryString = query.getQuery();
		MonetDBFeatureInputStream ifs = new MonetDBFeatureInputStream(connection, queryString);
		return ifs;
	}

	@Override
	public void close() throws DataStoreException {
		try {
			connection.close();
		}
		catch (Exception ex) { throw new DataStoreException(ex); }
	}

	@Override
	public boolean isClosed() throws DataStoreException {
		try {
			return connection.isClosed();
		} catch (SQLException e) {
			throw new DataStoreException(e);
		}
	}


}
