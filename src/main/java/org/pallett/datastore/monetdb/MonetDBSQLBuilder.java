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

import com.vividsolutions.jts.geom.Envelope;
import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jump.datastore.FilterQuery;
import com.vividsolutions.jump.datastore.SpatialReferenceSystemID;

/**
 * Creates SQL query strings for a PostGIS database
 */
public class MonetDBSQLBuilder {

  private SpatialReferenceSystemID defaultSRID = null;
  private String[] colNames = null;

  public MonetDBSQLBuilder(SpatialReferenceSystemID defaultSRID, String[] colNames) {
    this.defaultSRID = defaultSRID;
    this.colNames = colNames;
  }

  public String getSQL(FilterQuery query) {
    return buildQueryString(query);
  }

  private String buildQueryString(FilterQuery query) {
    StringBuilder qs = new StringBuilder();
    //HACK
    qs.append("SELECT ");
    qs.append(getColumnListSpecifier(colNames, query.getGeometryAttributeName()));
    qs.append(" FROM ");
    // fixed by mmichaud on 2010-05-27 for mixed case dataset names
    qs.append("\"").append(query.getDatasetName().replaceAll("\\.","\".\"")).append("\"");
    qs.append(" t WHERE ");
    qs.append(buildBoxFilter(query.getGeometryAttributeName(), query.getSRSName(), query.getFilterGeometry()));

    String whereCond = query.getCondition();
    if (whereCond != null) {
      qs.append(" AND ");
      qs.append(whereCond);
    }
    int limit = query.getLimit();
    if (limit != 0 && limit != Integer.MAX_VALUE) {
      qs.append(" LIMIT ").append(limit);
    }
    System.out.println(qs);
    return qs.toString();
  }

  private String buildBoxFilter(String geometryColName, SpatialReferenceSystemID SRID, Geometry geom) {
    Envelope env = geom.getEnvelopeInternal();

    StringBuilder buf = new StringBuilder();
    
    StringBuilder box = new StringBuilder();   
    box.append("POLYGON((");
    box.append(env.getMinX()).append(" ").append(env.getMinY()).append(","); // bottom-left corner
    box.append(env.getMaxX()).append(" ").append(env.getMinY()).append(","); // bottom-right corner
    box.append(env.getMaxX()).append(" ").append(env.getMaxY()).append(","); // top-right corner
    box.append(env.getMinX()).append(" ").append(env.getMaxY()).append(","); // top-left corner
    box.append(env.getMinX()).append(" ").append(env.getMinY()); // bottom-left corner again (finished polygon)
    box.append("))");    
        
    buf.append("mbroverlaps(mbr(\"").append(geometryColName).append("\"), mbr(PolyFromText('");
    buf.append(box);
    buf.append("', ");

    String srid = getSRID(SRID);
    srid = srid==null? "SRID(\"" + geometryColName + "\")" : srid;
    buf.append(srid).append(")))");
    
    return buf.toString();
  }

  private String getSRID(SpatialReferenceSystemID querySRID) {
    SpatialReferenceSystemID srid = defaultSRID;
    if (! querySRID.isNull())
      srid = querySRID;

    if (srid.isNull() || srid.getString().trim().length()==0)
      return null;
    else
      return srid.getString();
  }

  private String getColumnListSpecifier(String[] colNames, String geomColName) {
    // Added double quotes around each column name in order to read mixed case table names
    // correctly [mmichaud 2007-05-13]
    StringBuilder buf = new StringBuilder();
    // fixed by mmichaud using a patch from jaakko [2008-05-21]
    // query geomColName as geomColName instead of geomColName as geomColName + "_wkb"
    buf.append("\"").append(geomColName).append("\"");
    for (String colName : colNames) {
      if (! geomColName.equalsIgnoreCase(colName)) {
        buf.append(",\"").append(colName).append("\"");
      }
    }
    return buf.toString();
  }
}
