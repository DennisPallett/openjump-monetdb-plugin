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