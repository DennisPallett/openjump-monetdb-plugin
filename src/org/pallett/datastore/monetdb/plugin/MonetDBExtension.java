package org.pallett.datastore.monetdb.plugin;

import org.pallett.datastore.monetdb.MonetDBDataStoreDriver;

import com.vividsolutions.jump.datastore.DataStoreDriver;
import com.vividsolutions.jump.workbench.plugin.Extension;
import com.vividsolutions.jump.workbench.plugin.PlugInContext;

public class MonetDBExtension extends Extension {

	private static String extensionname = "MonetDBExtension";

	public void configure(PlugInContext context) throws Exception {
	    try {
	        context.getWorkbenchContext().getRegistry().createEntry(
	                DataStoreDriver.REGISTRY_CLASSIFICATION, new MonetDBDataStoreDriver());
	    } catch (Throwable e) {
	        context.getWorkbenchFrame().warnUser("MonetDBDataStoreDriver not loaded");
	        context.getErrorHandler().handleThrowable(e);
	    }
		
		new MonetDBPlugin().initialize(context);
	}

}