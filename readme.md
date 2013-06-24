Asynchronous HBase
==================
This is an alternative Java library to use HBase in applications that require
a fully asynchronous, non-blocking, thread-safe, high-performance HBase API.

This HBase client differs significantly from HBase's client (HTable).
Switching to it is not easy as it requires to rewrite all the code that was
interacting with any HBase API.  This pays off in applications that are
asynchronous by nature or that want to use several threads to interact
efficiently with HBase.

Please read the Javadoc starting from the HBaseClient class.  This class
replaces all your HTable instances.  Unlike HTable, you should have only
one instance of HBaseClient in your application, regardless of the number
of tables or threads you want to use.  The Javadoc also spells out rules
you have to follow in order to use the API properly in a multi-threaded
application.

Compiling with M7 client support
--------------------------------
Clone and checkout the "<asynchbase>-mapr" tag or branch of the AsyncHBase
release from MapR's github repository (https://github.com/mapr/asynchbase).
For example if you want to compile version 1.5.0, checkout the "1.5.0-mapr"
branch.

```bash
$ git clone git@github.com:mapr/asynchbase.git
$ cd asynchbase
$ git checkout v1.5.0-mapr
$ mvn clean install -DskipTests
```
