package org.pallett.datastore.monetdb;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.vividsolutions.jts.geom.Envelope;
import com.vividsolutions.jts.io.WKBReader;
import com.vividsolutions.jump.datastore.DataStoreMetadata;
import com.vividsolutions.jump.datastore.GeometryColumn;
import com.vividsolutions.jump.datastore.PrimaryKeyColumn;
import com.vividsolutions.jump.datastore.SpatialReferenceSystemID;
import com.vividsolutions.jump.datastore.jdbc.JDBCUtil;
import com.vividsolutions.jump.datastore.jdbc.ResultSetBlock;

public class MonetDBDataStoreMetadata implements DataStoreMetadata {
	private final WKBReader reader = new WKBReader();

	private MonetDBDataStoreConnection conn;

	private Map sridMap = new HashMap();
	
	public MonetDBDataStoreMetadata (MonetDBDataStoreConnection conn) {
		this.conn = conn;
	}

	@Override
	public String[] getDatasetNames() {
		final List datasetNames = new ArrayList();
		
		// Spatial tables only.
		JDBCUtil.execute(
				conn.getConnection(),
				"SELECT DISTINCT f_table_schema, f_table_name FROM geometry_columns",
				new ResultSetBlock() {
					public void yield( ResultSet resultSet ) throws SQLException {
						while ( resultSet.next() ) {
							String schema = resultSet.getString( 1 );
							String table = resultSet.getString( 2 );
							if ( !schema.equalsIgnoreCase( "sys" ) ) {
								table = schema + "." + table;
							}
							datasetNames.add( table );
						}
					}
				} );
		return ( String[] ) datasetNames.toArray( new String[]{} );
	}
	
	@Deprecated
	public SpatialReferenceSystemID getSRID(String tableName, String colName)
			throws SQLException {
		String key = tableName + "#" + colName;
		if (!sridMap.containsKey(key)) {
			// not in cache, so query it
			String srid = querySRID(tableName, colName);
			sridMap.put(key, new SpatialReferenceSystemID(srid));
		}
		return (SpatialReferenceSystemID)sridMap.get(key);
	}
	 
	 @Deprecated
	 private String querySRID(String tableName, String colName) {
		 final StringBuffer srid = new StringBuffer();

		 String[] tokens = tableName.split("\\.", 2);
		 String schema = tokens.length==2?tokens[0]:"sys";
		 String table = tokens.length==2?tokens[1]:tableName;
		 
		 String sql = "SELECT srid FROM geometry_columns where (f_table_schema = '" + schema + "' and f_table_name = '" + table + "')";
		 
		 JDBCUtil.execute(conn.getConnection(), sql, new ResultSetBlock() {
			 public void yield(ResultSet resultSet) throws SQLException {
				 if (resultSet.next()) {
					 srid.append(resultSet.getString(1));
				 }
			 }
		 });

		 return srid.toString();
	 }

	@Override
	public List<GeometryColumn> getGeometryAttributes(String datasetName) {
		final List<GeometryColumn> geometryAttributes = new ArrayList<GeometryColumn>();
		String sql = "SELECT \"f_geometry_column\", \"srid\", \"type\" FROM geometry_columns "
				+ geomColumnMetadataWhereClause( "f_table_schema", "f_table_name", datasetName );
		JDBCUtil.execute(
				conn.getConnection(), sql,
				new ResultSetBlock() {
					public void yield( ResultSet resultSet ) throws SQLException {
						while ( resultSet.next() ) {
							geometryAttributes.add(new GeometryColumn(
									resultSet.getString(1),
									resultSet.getInt(2),
									resultSet.getString(3)));
						}
					}
				} );
		return geometryAttributes;
	}

	@Override
	public List<PrimaryKeyColumn> getPrimaryKeyColumns(String datasetName) {
		final List<PrimaryKeyColumn> identifierColumns = new ArrayList<PrimaryKeyColumn>();

		String sql =
				" SELECT columns.name, columns.type" +
				" FROM sys.keys" +
				" INNER JOIN sys.tables ON keys.table_id = tables.id" +
				" INNER JOIN sys.schemas ON tables.schema_id = schemas.id" +
				" INNER JOIN sys.dependencies ON depend_id = keys.id" +
				" INNER JOIN sys.columns ON dependencies.id = columns.id" +
				geomColumnMetadataWhereClause( "schemas.name", "tables.name", datasetName ) +
				" AND keys.type = 0;";

		JDBCUtil.execute(
				conn.getConnection(), sql,
				new ResultSetBlock() {
					public void yield( ResultSet resultSet ) throws SQLException {
						while ( resultSet.next() ) {
							identifierColumns.add(new PrimaryKeyColumn(
									resultSet.getString(1),
									resultSet.getString(2)));
						}
					}
				} );
		return identifierColumns;
	}

	@Override
	public Envelope getExtents(String datasetName, String attributeName) {
		// TODO Auto-generated method stub
		return null;
	}
	
	public String[] getColumnNames( String datasetName ) {
		String sql = "SELECT columns.name FROM sys.columns" +
					 " INNER JOIN sys.tables ON columns.table_id = tables.id" +
					 " INNER JOIN sys.schemas ON tables.schema_id = schemas.id " +
					 geomColumnMetadataWhereClause( "schemas.name", "tables.name", datasetName );
		
		ColumnNameBlock block = new ColumnNameBlock();
		JDBCUtil.execute( conn.getConnection(), sql, block );
		
		return block.colName;
	}

	private String geomColumnMetadataWhereClause( String schemaCol, String tableCol, String tableName ) {
		int dotPos = tableName.indexOf( "." );
		String schema = "sys";
		String table = tableName;
		
		if (dotPos != -1) {
			schema = tableName.substring( 0, dotPos );
			table = tableName.substring( dotPos + 1 );
		}
		
		return "WHERE " + schemaCol + " = '" + schema + "'"
		+ " AND " + tableCol + " = '" + table + "'";
	}
	
	private static class ColumnNameBlock implements ResultSetBlock {
	    List colList = new ArrayList();
	    String[] colName;

	    public void yield( ResultSet resultSet ) throws SQLException {
	      while ( resultSet.next() ) {
	        colList.add( resultSet.getString( 1 ) );
	      }
	      colName = ( String[] ) colList.toArray(new String[colList.size()]);
	    }
	  }



}
