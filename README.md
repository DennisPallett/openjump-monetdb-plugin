OpenJUMP MonetDB DataStore Plugin
=======================

Plugin installation
-----------
To install the plugin copy the plugin jar (available here TODO) into the following directory:

```myOpenjumpFolder\lib\ext```

Now the plugin should be available when starting OpenJUMP.

Development
-----------
To work on this plugin Maven is used to setup the project and its dependencies.

First install the dependencies with the following command-line command:

```mvn clean install```

If this is successful create an Eclipse project with the following command:

```mvn eclipse:eclipse```

Now you can go to your Eclipse, go to File -> Import, and 'Import existing project' and select the directory of the plugin.

To compile your own JAR of the plugin run the following Eclipse command:

```mvn jar:jar```

This will create a new JAR file into the target directory.

Missing OpenJUMP dependency?
-----------
It is highly likely that the above instructions will fail due to a missing dependency, namely the OpenJUMP jar. To fix 
this you must download the OpenJUMP jar yourself (from http://www.openjump.org/) and import the OpenJUMP-X.X.X-*.jar 
into your local Maven repository with the following command:

```mvn install:install-file -Dfile=OpenJUMP-X.X.X-*.jar -DgroupId=org.openjump -DartifactId=OpenJUMP -Dversion=X.X.X -Dpackaging=pom```

Please take care to replace the necessary parts (X.X.X and the *) yourself with the correct strings. 

After you have run this command your local Maven repository should have now have the OpenJUMP dependency and Maven 
should be able to compile the plugin.
