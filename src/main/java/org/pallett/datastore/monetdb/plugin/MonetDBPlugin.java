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
package org.pallett.datastore.monetdb.plugin;

import com.vividsolutions.jump.workbench.plugin.PlugIn;
import com.vividsolutions.jump.workbench.plugin.PlugInContext;

/**
 * This plugin is a driver for a data source backed by a MonetDB/GeoSpatial database
 * 
 * @author Dennis Pallett <dennis@pallett.nl>
 */
public class MonetDBPlugin implements PlugIn {
	
  //debugging flag
  public static boolean DEBUG = true;
  
  public static PlugInContext plgInContext;
  
  /**
   * Initializes the plugin by creating the data source and data source 
   * query choosers.
   * @see PlugIn#initialize(com.vividsolutions.jump.workbench.plugin.PlugInContext)
   */
  public void initialize(PlugInContext context) {
	  System.out.println("MonetDBPlugin-init");
  }

  /**
   * This function does nothing, all the setup is completed in initialize().
   */
  public boolean execute(PlugInContext context) { return(false); }
  
  /**
   * @see PlugIn#getName()
   */
  public String getName() { return("MonetDB Driver" ); }
}