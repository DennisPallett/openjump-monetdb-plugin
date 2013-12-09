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
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import com.vividsolutions.jump.feature.Feature;
import com.vividsolutions.jump.feature.FeatureSchema;
import com.vividsolutions.jump.io.BaseFeatureInputStream;

public class MonetDBFeatureInputStream extends BaseFeatureInputStream {
	private FeatureSchema featureSchema;
	
	private Connection conn;
	
	private String queryString;
	
	private boolean initialized = false;
	
	private Exception savedException;
	
	private Statement stmt = null;
	
	private ResultSet rs = null;
	
	private MonetDBResultSetConverter mapper;

	public MonetDBFeatureInputStream(Connection conn, String queryString) {
		this.conn = conn;
		this.queryString = queryString;
	}
	
	public Connection getConnection()  {
		return conn;
	}
	
	private void init()	throws SQLException	{
		if (initialized) return;
		
		initialized = true;

		stmt = conn.createStatement();
		
		String parsedQuery = queryString;
		
		rs = stmt.executeQuery(parsedQuery);
		mapper = new MonetDBResultSetConverter(conn, rs);
		featureSchema = mapper.getFeatureSchema();
	}
	
	private Feature getFeature() throws Exception {
		return mapper.getFeature();
	}
	
	@Override
	public FeatureSchema getFeatureSchema() {
		if (featureSchema != null)
			return featureSchema;

		try {
			init();
		}
		catch (SQLException ex)
		{
			savedException = ex;
		}
		
		if (featureSchema == null) featureSchema = new FeatureSchema();
		
		return featureSchema;
	}

	@Override
	protected Feature readNext() throws Exception {
		if (savedException != null)
			throw savedException;
		if (! initialized)
			init();
		if (rs == null)
			return null;
		if (! rs.next())
			return null;
		
		return getFeature();
	}

	@Override
	public void close() throws Exception {
		if (rs != null) {
			rs.close();
		}
		if (stmt != null) {
			stmt.close();
		}		
	}



}
