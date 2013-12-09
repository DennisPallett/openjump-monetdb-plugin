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

import java.io.IOException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;

import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.io.ParseException;
import com.vividsolutions.jts.io.WKBReader;
import com.vividsolutions.jts.io.WKTReader;
import com.vividsolutions.jump.datastore.jdbc.ValueConverter;
import com.vividsolutions.jump.datastore.jdbc.ValueConverterFactory;
import com.vividsolutions.jump.feature.AttributeType;

public class MonetDBValueConverterFactory {

	// should lazily init these
	private final ValueConverter WKT_GEOMETRY_MAPPER = new WKTGeometryValueConverter();

	private final Connection conn;
	private final WKBReader wkbReader = new WKBReader();
	private final WKTReader wktReader = new WKTReader();

	public MonetDBValueConverterFactory (Connection conn) {
		this.conn = conn;
	}

	public ValueConverter getConverter(ResultSetMetaData rsm, int columnIndex)
	throws SQLException
	{
		String dbTypeName = rsm.getColumnTypeName(columnIndex).toLowerCase();
		
		// MD - this is slow - is there a better way?
		if (dbTypeName.equals("geometry") || dbTypeName.equals("point") || dbTypeName.equals("multipoint") || dbTypeName.equals("polygon") 
			|| dbTypeName.equals("multipolygon") || dbTypeName.equals("linestring") || dbTypeName.equals("multilinestring")) {
				return WKT_GEOMETRY_MAPPER;
		}

		// handle the standard types
		ValueConverter stdConverter = ValueConverterFactory.getConverter(rsm, columnIndex);
		if (stdConverter != null)
			return stdConverter;

		// default - can always show it as a string!
		return ValueConverterFactory.STRING_MAPPER;
	}

	class WKTGeometryValueConverter implements ValueConverter
	{
		public AttributeType getType() { return AttributeType.GEOMETRY; }
		public Object getValue(ResultSet rs, int columnIndex)
		throws IOException, SQLException, ParseException
		{
			Object valObj = rs.getObject(columnIndex);
			String s = valObj.toString();
			Geometry geom = wktReader.read(s);
			return geom;
		}
	}

	class WKBGeometryValueConverter implements ValueConverter
	{
		public AttributeType getType() { return AttributeType.GEOMETRY; }
		public Object getValue(ResultSet rs, int columnIndex)
		throws IOException, SQLException, ParseException
		{
			//Object obj = rs.getObject(columnIndex);
			//byte[] bytes = (byte[]) obj;
			byte[] bytes = rs.getBytes(columnIndex);
			Geometry geom = wkbReader.read(bytes);
			return geom;
		}
	}

}
