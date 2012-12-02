SQLRunner   v1.0.10 ALPHA

for tweaks see doc/ folder.

SQLRunner (Data Warehouse SQL Tool) is a command line database query tool, designed for rapid prototyping and easy automation of database access.

By default, the tool is quiet, and only outputs the results of queries.

To run it in console mode:


sqlrunner db=dbname user=dbuser pass=dbpass console head -


The parameter structure is modeled after Oracle's sqlplus, ie:

def myvar='somevalue';

select * from blah where glah='&&myvar';

UNIX INSTALLATION

Just set PATH to SQLRunner/bin folder, 
dump JDBC .jar files into SQLRunner/lib folder

WINDOWS INSTALLATION ISSUES

In order for everything to work, you need to set:

SQLRUNNER_HOME environment variable, ie:

SET SQLRUNNER_HOME=c:\jdk\SQLRunner

Or right-click on 'My Computer', go to 'Properties', then 'Advanced', then 'Environment Variables', and add a new environment variable SQLRUNNER_HOME, with value being the directory where SQLRunner is installed.

