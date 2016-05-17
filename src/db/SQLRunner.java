/*
 * SQLRunner.java
 *
 * program to run sql scripts with parameters; similar syntax to sqlplus.
 *
 * Thu Feb  7 02:39:03 EST 2005
 * 
 * Copyright (c) 2005, Alex Sverdlov
 * 
 * This work is free software; you can redistribute it and/or modify it
 * under the terms of the GNU General Public License as published by 
 * the Free Software Foundation, version 2, or higher.
 */

package db;

import java.util.*;
import java.text.*;
import java.sql.*;
import java.io.*;
import java.util.regex.*;
import java.util.zip.*;

/**
 * SQLRunner
 *
 * Java utility to run SQL, dump data to CSV, etc.
 *
 * @author Alex Sverdlov
 * @version 1.0.18
 */
public class SQLRunner {

    /**
     * connections we keep around.
     */
    private static TreeMap<String,Connection> dbconnections = new TreeMap<String,Connection>();

    /**
     * files currently being processed (to avoid infinite recursion).
     */
    private static Stack<String> processingFiles = new Stack<String>();

    /**
     * timestamp format in the prompt; yyyyMMdd hh:mm:ss
     */
    private static Format timestampFormat = new SimpleDateFormat("HH:mm:ss.SSS");
    
    /**
     * all property setting and getting should be done via setProperty and getProperty methods
     */
    private static Properties props = System.getProperties();

    /**
     * set property
     */
    public static void setProperty(String k,String v){
        props.setProperty(k,v);
    }
    public static void clearProperty(String k){
        props.remove(k);
    }
    /**
     * if not found, attempt to return _default, if again not found, return alt.
     */
    public static String getProperty(String n,String alt){
        return props.getProperty(n,props.getProperty(n+"_default",alt));
    }

    /**
     * get property with evaluated value.
     */
    public static String getEvalProperty(String n,String alt){
        return evalString(getProperty(n,alt));
    }

    /**
     * trim string from the right.
     */
    public static String rtrim(String s){
        if(s == null)
            return null;
        int i = s.length() - 1;
        if(i < 0)
            return "";
        while(i >= 0 && Character.isWhitespace(s.charAt(i)))
            i--;
        return s.substring(0,i+1);
    }

    /**
     * eval a string.
     */
    public static String evalString(String val) {
        StringBuffer sb = new StringBuffer();
        boolean found = true;
        if(val == null)
            return val;
        String[] ptrn = {
            "<\\?=\\s*\\$?(\\w+)\\s*\\?>",      // <?=$somevariable?> 
            "\\\\?\\&\\&(\\w+)\\.?",           // &&tdate, \&tdate
            "\\$\\{\\s*(\\w+)\\s*\\}\\.?"      // ${variable} 
            // "\\$(\\w+)\\.?"                     // $variable 
        };

        while(found){
            found = false;
            for(String p : ptrn){
                sb.setLength(0);
                Matcher matcher = Pattern.compile(p,Pattern.CASE_INSENSITIVE | Pattern.DOTALL).matcher(val);
                while (matcher.find()) {
                    String s = getEvalProperty(matcher.group(1),null);
                    if(s == null)
                        throw new IllegalStateException("unable to evaluate ["+val+"]. ('"+matcher.group(1)+"' is undefined).");
                    found = true;                    
                    matcher.appendReplacement(sb,Matcher.quoteReplacement(s));
                }
                matcher.appendTail(sb);
                val = sb.toString();
            }
        }
        {       // &tdate, -- this replacement does not cause an error if not found
            sb.setLength(0);
            Matcher matcher = Pattern.compile("(\\&(\\w+)\\.?)",Pattern.CASE_INSENSITIVE | Pattern.DOTALL).matcher(val);
            while (matcher.find()) {
                String s = getEvalProperty(matcher.group(2),null);
                if(s == null){
                    s = matcher.group(1);       // leave alone in case not found.
                }else{
                    found = true;
                }
                matcher.appendReplacement(sb,Matcher.quoteReplacement(s));
            }
            matcher.appendTail(sb);
            val = sb.toString();
        }
        return found ? evalString(val) : val;
    }

    /**
     * execute a shell command
     */
    public static String executeShellCommand(String cmd) {
        StringBuffer sb = new StringBuffer();
        Process p;
        try {
            String[] cmd2 = {"/bin/sh", "-c", cmd};
            p = Runtime.getRuntime().exec(cmd2);
            p.waitFor();
            BufferedReader br = new BufferedReader(new InputStreamReader(p.getInputStream()));
            String line = "";
            while((line = br.readLine()) != null){
                sb.append(line + "\n");
            }
        }catch(Exception e){
            e.printStackTrace();
        }
        return sb.toString().trim();
    }

    /**
     * Run sql query
     */
    public static void sqlRun(String sql,final PrintStream ps) throws Exception {
        long qtim = 0,otim = 0;
        boolean isdatetimestamp = false;

        setProperty("_current_connection_laststmnt_raw",sql.trim());
        sql = evalString(sql);
        setProperty("_current_connection_laststmnt",sql.trim());

        //
        // if nothing to run, return.
        //
        if(sql.trim().length() <= 0)
            return;

        if(getEvalProperty("env","off").equals("on"))
            props.list(System.out);

        qtim = System.currentTimeMillis();
        setProperty("_current_connection_laststmnt_startmillis",""+qtim);

        if(getEvalProperty("log","off").equals("on")){
            System.out.println("\n--SQL START: "+new java.util.Date(qtim));
            System.out.println("-----------------------------------");
            System.out.println( sql + ";" );
            System.out.println("-----------------------------------");
        }

        if(getEvalProperty("pretend","off").equals("on") || getEvalProperty("debug","off").equals("on")){
            if(getEvalProperty("log","off").equals("on"))
                System.out.println("-- In pretend mode. Not running query.");
            return;
        }

        // connection key (catenated connection info).
        StringBuffer key = new StringBuffer();

        // if db name is "blah123", then use blah123_user for user, etc. (unless `user' is defined).
        for(String sufix : new String[]{"url","user","pass","driver"}){
            String val = getEvalProperty(sufix,null);
            if(val == null)
                val = getEvalProperty(getEvalProperty("db","db")+"_"+sufix,null);
            
            // if db_pass_cmd is defined, then run this command to get password.
            if(val == null || val.trim().length() == 0){
                //if(getEvalProperty("log","off").equals("on"))
                //    System.out.println("-- "+sufix+" is not defined.");
                
                // if value is null, check if _cmd is defined.
                String cmd = getEvalProperty(getEvalProperty("db","db")+"_"+sufix+"_cmd",
                    getEvalProperty(sufix+"_cmd",null));
                if(cmd != null){
                    if(getEvalProperty("log","off").equals("on"))
                        System.out.println("-- running: "+cmd+" to define "+sufix);
                    val = executeShellCommand(cmd);
                    // DO NOT log the value.
                }
            }

            // we -must- have url,user,pass,driver defined in environment.
            if(val == null)
                throw new IllegalStateException("undefined db ("+sufix+") connection info");

            setProperty("_current_connection_"+sufix,val);

            // append to connection key (changing connection details will start a new connection).
            key.append(val+"|");
        }
        // note: default driver should probably be "sun.jdbc.odbc.JdbcOdbcDriver"


        // see if we have this connection saved
        Connection connection = dbconnections.get(key.toString());
        if(connection == null){

            String driverclass = getProperty("_current_connection_driver",null);
            if( driverclass.toLowerCase().indexOf("oracle") >= 0 ){
                isdatetimestamp = true;
            }
            //
            // init driver.
            // 
            Class.forName( driverclass ).newInstance();

            boolean found_cntn_props = false;
            // loop for all properties, find jdbc connection properties for current connection
            Properties cntn_props = new Properties();
            {
                String prefix = "param_";
                for( String k : props.stringPropertyNames() ){
                    if(k.startsWith(prefix)){
                        found_cntn_props = true;
                        cntn_props.setProperty(k.substring(prefix.length()), getEvalProperty(k,null) );
                    }
                }
                prefix = getEvalProperty("db","db")+"_param_";
                for( String k : props.stringPropertyNames() ){
                    if(k.startsWith(prefix)){
                        found_cntn_props = true;
                        cntn_props.setProperty(k.substring(prefix.length()), getEvalProperty(k,null) );
                    }
                }
            }

            
            if(found_cntn_props){
                if(getEvalProperty("log","off").equals("on")){
                    System.out.println("-- found connection parameters; using alt connection method: "+getProperty("_current_connection_url",null));
                    for( String k : cntn_props.stringPropertyNames() )
                       System.out.println("-- param: "+k+"="+cntn_props.get(k));
                }
                cntn_props.setProperty("user",getProperty("_current_connection_user",null));
                cntn_props.setProperty("password",getProperty("_current_connection_pass",null));
                // 
                // connect.
                //
                connection = DriverManager.getConnection(
                    getProperty("_current_connection_url",null),
                    cntn_props
                );

            }else{
                if(getEvalProperty("log","off").equals("on"))
                    System.out.println("-- using classic connection method: "+getProperty("_current_connection_url",null)); 

                // 
                // connect, using classic mode.
                //
                connection = DriverManager.getConnection(
                    getProperty("_current_connection_url",null),
                    getProperty("_current_connection_user",null),
                    getProperty("_current_connection_pass",null)
                );
            }
            
            // save this connection for later use.
            dbconnections.put(key.toString(),connection);
        }

        //
        // to be initialized below.
        ResultSet rs = null;
        String[] m = new String[1];

        //
        // if user is doing a metadata lookup.
        //
        if(strmatch(sql,"^\\s*show\\s+table(s)?\\s*;?\\s*$") != null){
            DatabaseMetaData databaseMetaData = connection.getMetaData();
            // get all tables (uses jdbc)
            rs = databaseMetaData.getTables(
                    getEvalProperty("catalog",null),
                    getEvalProperty("schema",null),null,
                    new String[]{"TABLE"}
                );
        }else if(strmatch(sql,"^\\s*show\\s+view(s)?\\s*;?\\s*$") != null){
            DatabaseMetaData databaseMetaData = connection.getMetaData();
            // get all tables (uses jdbc)
            rs = databaseMetaData.getTables(
                    getEvalProperty("catalog",null),
                    getEvalProperty("schema",null),null,
                    new String[]{"VIEW"}
                );
        }else if(strmatch(sql,"^\\s*show\\s+object(s)?\\s*;?\\s*$") != null){
            DatabaseMetaData databaseMetaData = connection.getMetaData();
            rs = databaseMetaData.getTables(
                    getEvalProperty("catalog",null),
                    getEvalProperty("schema",null),null,
                    new String[]{"VIEW","TABLE"}
                );
        }else if(strmatch(sql,"^\\s*show\\s+catalog(s)?\\s*;?\\s*$") != null){
            DatabaseMetaData databaseMetaData = connection.getMetaData();
            rs = databaseMetaData.getCatalogs();
        }else if(strmatch(sql,"^\\s*show\\s+schema(s)?\\s*;?$") != null){
            DatabaseMetaData databaseMetaData = connection.getMetaData();
            rs = databaseMetaData.getSchemas();
        }else if(strmatch(sql,"^\\s*show\\s+table\\s+type(s)?\\s*;?\\s*$") != null){
            DatabaseMetaData databaseMetaData = connection.getMetaData();
            rs = databaseMetaData.getTableTypes();
        }else if((m = strmatch(sql,"^\\s*desc(ribe)?\\s+(\\S+)\\s*;?\\s*$")) != null){
            DatabaseMetaData databaseMetaData = connection.getMetaData();
            rs = databaseMetaData.getColumns(props.getProperty("catalog"),
                    props.getProperty("schema"),
                        ( databaseMetaData.storesLowerCaseIdentifiers() ? m[2].toLowerCase() : m[2].toUpperCase()),null);
        }else if(strmatch(sql,"^\\s*info\\s*;?$") != null){
            //
            // if in console mode, display database info.
            //
            if(ps != null){ 
                DatabaseMetaData databaseMetaData = connection.getMetaData();
                try { ps.println("Driver: "+databaseMetaData.getDriverName()); }catch(Exception e){}
                try { ps.println("Driver Version: "+databaseMetaData.getDriverVersion()); }catch(Exception e){}
                try { ps.println("JDBC Version: "+databaseMetaData.getJDBCMajorVersion()+"."+
                    databaseMetaData.getJDBCMinorVersion()); }catch(Exception e){}
                try { ps.println("Database: "+databaseMetaData.getDatabaseProductName()); }catch(Exception e){}
                try { ps.println("Database Version: "+databaseMetaData.getDatabaseProductVersion()); }catch(Exception e){}
                try { ps.println("URL: "+databaseMetaData.getURL()); }catch(Exception e){}
                try { ps.println("User: "+databaseMetaData.getUserName()); }catch(Exception e){}
                try { ps.println("SQLRunner Version: "+props.getProperty("version")); }catch(Exception e){}
            }
        }else{
            // user is doing a db query. 

            // for select statements, set autocommit to off.
            if(strmatch(sql,"^\\s*select") != null){
                try{ connection.setAutoCommit(false); }catch(Exception e){}
            }else{
                // for non-select statements, set autocommit to variable name, defaulting to "on".
                try{ connection.setAutoCommit( getEvalProperty("autocommit","on").equals("on") ); }catch(Exception e){}
            }

            // 
            // execute statement
            //
            final Statement statement = connection.createStatement();
            try { statement.setFetchSize(1024*16);  }catch(Exception e){}  // random large number.
            // statement.closeOnCompletion();   jdk 1.7 feature

            // attempt to setup timeout
            try {
                statement.setQueryTimeout(Integer.parseInt(getEvalProperty("timeout","0")));
            }catch(java.sql.SQLFeatureNotSupportedException e){ 
                if(getEvalProperty("log","off").equals("on")){
                    System.out.println("-- Timeout feature not supported by database driver.");
                }
            }catch(Exception e){
                if(getEvalProperty("log","off").equals("on")){
                    System.out.println("-- Timeout feature not supported by database driver.");
                    System.out.println("-- also, stupid "+getProperty("_current_connection_driver","database")+" driver doesn't know about SQLFeatureNotSupportedException");
                }
            }




            Thread shutdownhook = new Thread() {
                @Override
                public void run() {                    
                    try { 
                        if(getEvalProperty("log","off").equals("on"))
                            System.out.println("-- canceling statement");
                        statement.cancel(); 
                    } catch (SQLException e){ 
                        if(getEvalProperty("log","off").equals("on"))
                            System.out.println(e.getMessage());
                    }
                    try { 
                        if(getEvalProperty("log","off").equals("on"))
                            System.out.println("-- closing statement");
                        statement.close(); 
                    } catch (SQLException e){ 
                        if(getEvalProperty("log","off").equals("on"))
                            System.out.println(e.getMessage());                    
                    }
                    // close spool file if not stdout
                    if(ps != null && ps != System.out)
                        ps.close();                    
                }
            };


            boolean rsout = false; 

            try {
                // cleanup if killed.
                Runtime.getRuntime().addShutdownHook(shutdownhook); 
                rsout = statement.execute(sql);
            } catch(java.lang.Exception e){
                if( // Postgres
                    e.getMessage().indexOf("Unable to interpret the update count in command completion tag") >= 0 ||
                    // Netezza
                    e.getMessage().indexOf("Unable to fathom update count") >= 0 ||
                    // Netezza (new driver)
                    e.getMessage().indexOf("The update count exceeded Integer") >= 0
                    ){
                    // in PostgreSQL 9.1 JDBC4 (build 901)
                    // ...and Netezza 
                    // the driver appears to be doing:
                    // update_count = Integer.parseInt(...); 
                    // so if inserting/updating > 2^31 records, we blow up for no good reason.
                    if(getEvalProperty("log","off").equals("on")){
                        System.out.println("-- SUPPRESSED JAVA EXCEPTION: "+e.getMessage());
                    }
                }else{
                    throw e;
                }
            }finally{
                // no need to cleanup now :-)
                Runtime.getRuntime().removeShutdownHook(shutdownhook);
            }

            otim = System.currentTimeMillis();
            // record (just in case).
            setProperty("_current_connection_laststmnt_rsmillis",""+otim);                
            setProperty("_current_connection_laststmnt_rsseconds",""+(otim - qtim)/1000.0);

            if(getEvalProperty("log","off").equals("on")){
                double d = ((otim - qtim) / 1000.0) / 60;
                System.out.printf("\n--SQL END: "+new java.util.Date(otim)+"; %.2f min\n",d);
                if(!rsout){
                    // jdbc4 has a long (not int) version of this method.
                    // too bad drivers are stupid and fail at the execute step,
                    // and not during getUpdateCount step.
                    System.out.println("-- UpdateCount: "+statement.getUpdateCount());
                }
                System.out.println("-----------------------------------");
            }

            if(!rsout){
                // 
                // if no result set, then simply return.
                //
                if(getEvalProperty("console","off").equals("on")){
                    System.out.println("OK (no results)");
                }
                statement.close();
                return ;
            }
            
            // 
            // get the results
            //
            rs = statement.getResultSet();
        }

        
        if(rs != null)
            resultSetOutput(rs,ps,isdatetimestamp);

        long etim = System.currentTimeMillis();
        
        // record (just in case).
        setProperty("_current_connection_laststmnt_millis",""+etim);
        setProperty("_current_connection_laststmnt_seconds",""+(etim - qtim)/1000.0);
         
        if(getEvalProperty("log","off").equals("on")){
            double d = ((etim - qtim) / 1000.0)/60;
            if(otim > 0){   // there was output
                double e = ((etim - otim) / 1000.0)/60;
                System.out.printf("\n--OUT END: "+new java.util.Date(etim)+"; OUT: %.2f min, TOTAL: %.2f min\n",e,d);
            }else{
                System.out.printf("\n--OUT END: "+new java.util.Date(etim)+"; TOTAL: %.2f min\n",d);
            }
            System.out.println("-----------------------------------");
        }
        if(rs != null)
            rs.close();
    }


/*
 *
 *  // todo database loads via native database utilities.
    Process p = Runtime.getRuntime().exec("some executable");
    p.waitFor();
 *
 *
 */


    /**
     * customized date formatter, to allow for nanoseconds/microseconds output from java.sql.Timestamp objects.
     */
    @SuppressWarnings("serial")     
    public static Format dateFormatFactory(String pttrn){
        if(pttrn.indexOf("NS") >= 0){
            String[] ns = pttrn.split("NS");
            final Format[] fns = new Format[ns.length+1];
            for(int i=0;i<ns.length;i++)
                fns[i] = ns[i].length() > 0 ? (new SimpleDateFormat(ns[i])) : null;
            return new Format(){
                public StringBuffer format(Object obj, StringBuffer toAppendTo, FieldPosition pos) {
                    long nanos = 0;
                    if(obj instanceof java.sql.Timestamp)
                        nanos = ((java.sql.Timestamp)obj).getNanos();
                    Format[] lns = fns;
                    if(lns.length == 0)
                        toAppendTo.append(String.format("%09d",nanos));
                    else
                        for(int i=0;i<lns.length;i++){
                            if(lns[i] != null) toAppendTo.append(lns[i].format(obj));
                            if( i<lns.length-1 ) toAppendTo.append(String.format("%09d",nanos));
                        }
                    return toAppendTo;
                }
                public Object parseObject(String source, ParsePosition pos){return null;}
            };
        }
        if(pttrn.indexOf("US") >= 0){
            String[] ns = pttrn.split("US");
            final Format[] fns = new Format[ns.length+1];
            for(int i=0;i<ns.length;i++)
                fns[i] = ns[i].length() > 0 ? (new SimpleDateFormat(ns[i])) : null;
            return new Format(){
                public StringBuffer format(Object obj, StringBuffer toAppendTo, FieldPosition pos) {
                    long nanos = 0;
                    if(obj instanceof java.sql.Timestamp)
                        nanos = ((java.sql.Timestamp)obj).getNanos() / 1000;
                    Format[] lns = fns;
                    if(lns.length == 0)
                        toAppendTo.append(String.format("%06d",nanos));
                    else
                        for(int i=0;i<lns.length;i++){
                            if(lns[i] != null) toAppendTo.append(lns[i].format(obj));
                            if( i<lns.length-1 ) toAppendTo.append(String.format("%06d",nanos));
                        }
                    return toAppendTo;
                }
                public Object parseObject(String source, ParsePosition pos){return null;}
            };
        }
        return new SimpleDateFormat(pttrn);
    }




    /**
     * output data to a CSV file
     */
    public static void resultSetOutput(ResultSet rs,PrintStream ps,boolean isdatetimestamp) throws Exception {

        // 
        // setup record delimiter for output
        //
        String delim = getEvalProperty("delim",",");
        String textdelim = getEvalProperty("textdelim"," ");
        String linesep = getEvalProperty("linesep","\n");

        
        delim = delim.replaceAll("\\\\t","\t");
        delim = delim.replaceAll("\\\\n","\n");
        delim = delim.replaceAll("\\\\r","\r");

        textdelim = textdelim.replaceAll("\\\\t","\t");
        textdelim = textdelim.replaceAll("\\\\n","\n");
        textdelim = textdelim.replaceAll("\\\\r","\r");


        linesep = linesep.replaceAll("\\\\t","\t");
        linesep = linesep.replaceAll("\\\\n","\n");
        linesep = linesep.replaceAll("\\\\r","\r");

        // replace octals (e.g. \000, \1, etc.) 
        for(int i=0xFF;i>=0;i--){
            String octi = Integer.toOctalString(i);
            String octr = Character.toString((char)i);
            delim = delim.replaceAll("\\\\00"+octi,octr);
            textdelim = textdelim.replaceAll("\\\\00"+octi,octr);
            linesep = linesep.replaceAll("\\\\00"+octi,octr);
            delim = delim.replaceAll("\\\\0"+octi,octr);
            textdelim = textdelim.replaceAll("\\\\0"+octi,octr);
            linesep = linesep.replaceAll("\\\\0"+octi,octr);
            delim = delim.replaceAll("\\\\"+octi,octr);
            textdelim = textdelim.replaceAll("\\\\"+octi,octr);
            linesep = linesep.replaceAll("\\\\"+octi,octr);
        }


        // 
        // retrieve and figure out resultset metadata.
        //
        ResultSetMetaData meta = rs.getMetaData();
        int cols = meta.getColumnCount();
        boolean[] colIsDate = new boolean[cols+1];
        boolean[] colIsTimestamp = new boolean[cols+1];
        boolean[] colIsTime = new boolean[cols+1];
        boolean[] colIsDouble = new boolean[cols+1];        
        String[] colTypeName = new String[cols+1];
        String[] colTypeNameOrig = new String[cols+1];
        String[] colName = new String[cols+1];
        int[] colSiz = new int[cols+1];     // size
        int[] colPres = new int[cols+1];    // precision
        int[] colScal = new int[cols+1];    // scale 
        int[] colNul = new int[cols+1];     // is null
        int[] colCalcSize = new int[cols+1];    // calculate column length

        // 
        // populate metadata arrays (for output).
        //    NOW~TIMESTAMP~26~26~6~1
        for(int i=1;i<=cols;i++){
            if(getEvalProperty("uchead","off").equals("on") ){
                colName[i] = meta.getColumnName(i).toUpperCase();
            }else{
                colName[i] = meta.getColumnName(i).toLowerCase();
            }
                // HIVE does the stupid database.column notation for column names.
                String[] colNameArr = colName[i].split("\\.");
                colName[i] = colNameArr[ colNameArr.length - 1];

            colCalcSize[i] = colName[i].length();
            colTypeName[i] = meta.getColumnTypeName(i).toUpperCase();
            colTypeNameOrig[i] = meta.getColumnTypeName(i).toUpperCase();
            colSiz[i] = Math.max(meta.getColumnDisplaySize(i),0);
            colPres[i] = Math.max(meta.getPrecision(i),0);
            colScal[i] = Math.max(meta.getScale(i),0);
            colNul[i] = Math.max(meta.isNullable(i),0) > 0 ? 1 : 0;
            colIsDate[i] = meta.getColumnType(i) == Types.DATE ? true : false;
            if(meta.getColumnType(i) == Types.TIME){
                colIsTime[i] = true;
            }else if(meta.getColumnType(i) == Types.TIMESTAMP || meta.getColumnTypeName(i).toUpperCase().startsWith("TIME")){
                colIsTimestamp[i] = true;
            }
            colIsDouble[i] = meta.getScale(i) > 0 ? true : false;            
            String tm = colTypeName[i];

            // special case for Oracle (number without precision nor scale).
            // also special case for Greenplum; it too has numeric with 0s.
            if(colPres[i]==0 && colScal[i]==0){
                if(tm.matches(".*NUMERIC.*") || tm.matches(".*NUMBER.*") || 
                    tm.matches(".*DECIMAL.*") || tm.matches(".*DOUBLE.*") || 
                    tm.matches(".*FLOAT.*") || tm.matches(".*REAL.*")){
                        colIsDouble[i]=true;
                }
            }

            if(tm.matches(".*NUMERIC.*") || tm.matches(".*NUMBER.*")){
                colTypeName[i] = "NUMBER";
                colSiz[i] = 0;
            }else if(tm.matches(".*DECIMAL.*") || tm.matches(".*DOUBLE.*") || tm.matches(".*FLOAT.*") || tm.matches(".*REAL.*")){
                // if(colScal[i] == 0)
                //    colPres[i] = 0;
                if(tm.matches(".*DECIMAL.*") && colScal[i] == 0)
                    colIsDouble[i] = false;
                else
                    colIsDouble[i] = true;
                colTypeName[i] = "NUMBER";
                colSiz[i] = 0;
            }else if(tm.matches(".*BIGINT.*") || tm.matches(".*INTEGER.*") || tm.matches(".*SMALLINT.*") || tm.matches(".*TINYINT.*") || tm.matches(".*INT.*")){
                colTypeName[i] = "NUMBER";
                colSiz[i] = 0;
                colScal[i] = 0;
            }else if(tm.matches(".*VARCHAR.*")){
                colTypeName[i] = "VARCHAR";
                colPres[i] = colScal[i] = 0;
            }
        }
        
        // 
        // all dates are output in YYYYMMDD format.
        //
        Format dateFormat = dateFormatFactory(System.getProperty("dateformat","yyyyMMdd"));
        Format timestampFormat = dateFormatFactory(System.getProperty("timestampformat","yyyyMMdd HH:mm:ss.SSS"));
        Format timeFormat = dateFormatFactory(System.getProperty("timeformat","HH:mm:ss.SSS"));

        String outstr = "";
        


        // 
        // output loop
        //
        StringBuffer sb = new StringBuffer(10000);
        NumberFormat nf = NumberFormat.getInstance();
        nf.setGroupingUsed(false);
        nf.setMaximumFractionDigits(0xFF);
        nf.setMaximumIntegerDigits(0xFF);
        nf.setMinimumFractionDigits(0);
        nf.setMinimumIntegerDigits(1);

        long outcnt = 0;

        char delimChar = delim.charAt(0);
        char delimReplace = getEvalProperty("delimReplace"," ").charAt(0);

        Format[] formats = new Format[cols+1];
        for(int i=1;i<=cols;i++){
            if(System.getProperty(colName[i]+"_format") != null){
                formats[i] = dateFormatFactory(System.getProperty(colName[i]+"_format"));
            } else if(colIsDate[i]){
                formats[i] = dateFormat;
            } else if(colIsTime[i]){
                formats[i] = timeFormat;
            } else if(colIsTimestamp[i]){
                formats[i] = timestampFormat;
            }
        }

        String nullval = getEvalProperty("nullval","");

        // trim output strings (default, yes!)
        boolean outputTrimStrings = true;
        if(getEvalProperty("trim","on").equals("off")){
            outputTrimStrings = false;
        }
        
        // do not replace delimiter in output strings.
        boolean outputFixDelim = true;
        if(getEvalProperty("fixdelim","on").equals("off")){
            outputFixDelim = false;
        }
        

        //
        // determine output format
        //
        if(getEvalProperty("outformat","delim").equals("delim")){


            // 
            // do we display header (or full header) ?
            //  full header includes type, size, precision, scale, null 
            //  (ie: enough info to recreate table in another db).
            //
            if(getEvalProperty("head","off").equals("on") || getEvalProperty("fhead","off").equals("on")){
                StringBuffer b = new StringBuffer();
                boolean fullheader = getEvalProperty("fhead","off").equals("on");
                for(int i=1;i<=cols;i++){
                    b.append(colName[i]);
                    if(fullheader)
                        b.append('~'+colTypeName[i]+'~'+colSiz[i]+'~'+colPres[i]+'~'+colScal[i]+'~'+colNul[i]+'~'+colTypeNameOrig[i]);
                    b.append(delim);
                }
                b.setLength(b.length()-delim.length());
                outstr = b.toString();
                if(ps != null)
                    ps.print(outstr + linesep);
                setProperty("_current_connection_laststmnt_header",outstr);
            }

            while(rs.next()){
                outcnt++;
                for(int i=1;i<=cols;i++){
                    if(colIsDate[i]){        // if date, then format as YYYYMMDD
                        if(isdatetimestamp){   // oracle dates are timestamps.
                            java.sql.Timestamp d = rs.getTimestamp(i);
                            if(!rs.wasNull())
                                sb.append(formats[i].format(d));
                            else sb.append(nullval);
                        }else{
                            java.sql.Date d = rs.getDate(i);
                            if(!rs.wasNull())
                                sb.append(formats[i].format(d));
                            else sb.append(nullval);
                        }
                    } else if(colIsTimestamp[i]){        // if date, then format as YYYYMMDDHHMMSS.SSS
                        java.sql.Timestamp d = rs.getTimestamp(i);
                        if(!rs.wasNull())
                            sb.append(formats[i].format(d));
                        else sb.append(nullval);                        
                    } else if(colIsTime[i]){                // time column.
                        java.sql.Time d = rs.getTime(i);
                        if(!rs.wasNull())
                            sb.append(formats[i].format(d));
                        else sb.append(nullval);                        
                    } else if(colIsDouble[i]) {             // if number has decimal point.
                        double d = rs.getDouble(i);
                        if(!rs.wasNull())
                            sb.append(nf.format(d));
                        else sb.append(nullval);                        
                    } else {                        // treat everything else as string.
                        String s = rs.getString(i);
                        if(!rs.wasNull()){                            
                            if(outputFixDelim)
                                s = s.replace(delimChar,delimReplace);
                            if(outputTrimStrings)
                                s = s.trim();
                            sb.append(s);
                        }
                        else sb.append(nullval);                        
                    }
                    sb.append(delim);
                }
                sb.setLength(sb.length()-delim.length());   // add new line.
                outstr = sb.toString();
                if(ps != null)
                    ps.print(outstr + linesep);                       // output
                sb.setLength(0);                    // reset buffer.
            }
        } else if ( getEvalProperty("outformat","delim").equals("text") ){

            java.util.Vector<String[]> dataList = new java.util.Vector<String[]>();
            String[] spaces = new String[4096];
            spaces[0] = "";
            for(int i=1;i<spaces.length;i++){
                spaces[i] = spaces[i-1] + " ";
            }
            
            // Determine header
            String printHeader = getEvalProperty("head","off");
            boolean pageFormat=false;
            
            // Determine underline character
            String headUnderlineChar = getEvalProperty("head_underline", "-");
            
            // Determine pagesize
            int pageSize = Integer.parseInt(getEvalProperty("pagesize","2048"));

            while(rs.next()){
                outcnt++;
                
                //
                // do we display header?
                //
                if(printHeader.equals("on") &&
                    rs.getRow() % pageSize == 1 ){
                    dataList.addElement(colName);
                }
                
                String[] dataRecord = new String[cols+1];
                for(int i=1;i<=cols;i++){
                    if(colIsDate[i]){        // if date, then format as YYYYMMDD
                        if(isdatetimestamp){   // oracle dates are timestamps.
                            java.sql.Timestamp d = rs.getTimestamp(i);
                            if(!rs.wasNull())
                                dataRecord[i] = formats[i].format(d);
                            else dataRecord[i] = nullval;
                        }else{
                            java.sql.Date d = rs.getDate(i);
                            if(!rs.wasNull())
                                dataRecord[i] = formats[i].format(d);
                            else dataRecord[i] = nullval;
                        }
                    } else if(colIsTimestamp[i]){        // if date, then format as YYYYMMDDHHMMSS.SSS
                        java.sql.Timestamp d = rs.getTimestamp(i);
                        if(!rs.wasNull())
                            dataRecord[i] = formats[i].format(d);
                        else dataRecord[i] = nullval;
                    } else if(colIsTime[i]){                // time column.
                        java.sql.Time d = rs.getTime(i);
                        if(!rs.wasNull())
                            dataRecord[i] = formats[i].format(d);
                        else dataRecord[i] = nullval;
                    } else if(colIsDouble[i]) {             // if number has decimal point.
                        double d = rs.getDouble(i);
                        if(!rs.wasNull())
                            dataRecord[i] = nf.format(d);
                        else dataRecord[i] = nullval;
                    } else {                        // treat everything else as string.
                        String s = rs.getString(i);
                        if(!rs.wasNull())
                            dataRecord[i] = s;
                        else dataRecord[i] = nullval;
                    }
                    if(colCalcSize[i] < dataRecord[i].length() )
                        colCalcSize[i]=dataRecord[i].length();
                }
                dataList.addElement(dataRecord);
             
                if(dataList.size() > pageSize){
                    pageFormat=true;
                    int loopCnt = 1;
                    java.util.Enumeration<String[]> en = dataList.elements();
                    while(en.hasMoreElements()){
                        String[] r = en.nextElement();
                        // when processing first record (header)
                        if (loopCnt == 1 && printHeader.equals("on")){
                            // add a blank line 
                            if (rs.getRow()/pageSize != 1) 
                                ps.print(linesep);
                            // add header
                            for(int i=1;i<=cols;i++){
                                int d = colCalcSize[i] - r[i].length();
                                sb.append(r[i] + spaces[d]);
                                sb.append(textdelim);
                            }
                            sb.setLength(sb.length()-textdelim.length());   // add new line.
                            outstr = sb.toString();
                            if(ps != null)
                                ps.print(outstr + linesep);                       // output
                            sb.setLength(0);                    // reset buffer.
                            // add underline character
                            if(!headUnderlineChar.equals("none")){
                                for(int i=1;i<=cols;i++){
                                    sb.append(String.format("%"+colCalcSize[i]+"s", headUnderlineChar).replace(' ', headUnderlineChar.charAt(0)));
                                    sb.append(textdelim);
                                }
                                sb.setLength(sb.length()-textdelim.length());   // add new line.
                                outstr = sb.toString();
                                if(ps != null)
                                    ps.print(outstr + linesep);                       // output
                                sb.setLength(0);                    // reset buffer.
                            }
                        }
                        // print data
                        else {
                            for(int i=1;i<=cols;i++){
                                int d = colCalcSize[i] - r[i].length();
                                if(colIsDouble[i] || colCalcSize[i]==0){
                                    sb.append(spaces[d] + r[i]);
                                }else{
                                    sb.append(r[i] + spaces[d]);
                                }
                                sb.append(textdelim);
                            }
                            sb.setLength(sb.length()-textdelim.length());   // add new line.
                            outstr = sb.toString();
                            if(ps != null)
                                ps.print(outstr + linesep);                       // output
                            sb.setLength(0);                    // reset buffer.
                        }
                        loopCnt++;
                    }
                    dataList.clear();
                }
            }

            // residual records (or if total records < pagesize)
            int loopCnt = 1;
            java.util.Enumeration<String[]> en = dataList.elements();
            while(en.hasMoreElements()){
                String[] r = en.nextElement();
                // process first record
                if (loopCnt == 1 && printHeader.equals("on")) {
                    // add a blank line first
                    if (pageFormat)
                        ps.print(linesep);
                    // add header
                    for(int i=1;i<=cols;i++){
                        int d = colCalcSize[i] - r[i].length();
                        sb.append(r[i] + spaces[d]);
                        sb.append(textdelim);
                    }
                    sb.setLength(sb.length()-textdelim.length());   // add new line.
                    outstr = sb.toString();
                    if(ps != null)
                        ps.print(outstr + linesep);                       // output
                    sb.setLength(0);                    // reset buffer.
                    // add underline character
                    if(!headUnderlineChar.equals("none")){
                        for(int i=1;i<=cols;i++){
                            sb.append(String.format("%"+colCalcSize[i]+"s", headUnderlineChar).replace(' ', headUnderlineChar.charAt(0)));
                            sb.append(textdelim);
                        }
                        sb.setLength(sb.length()-textdelim.length());   // add new line.
                        outstr = sb.toString();
                        if(ps != null)
                            ps.print(outstr + linesep);                       // output
                        sb.setLength(0);                    // reset buffer.
                    }
                }
                // print data
                else {
                    for(int i=1;i<=cols;i++){
                        int d = colCalcSize[i] - r[i].length();
                        if(colIsDouble[i] || colSiz[i]==0){
                            sb.append(spaces[d] + r[i]);
                        }else{
                            sb.append(r[i] + spaces[d]);
                        }
                        sb.append(textdelim);
                    }
                    sb.setLength(sb.length()-textdelim.length());   // add new line.
                    outstr = sb.toString();
                    if(ps != null)
                        ps.print(outstr + linesep);                       // output
                    sb.setLength(0);                    // reset buffer.
                }
                loopCnt++;
            }
            dataList.clear();

        }else if(getEvalProperty("outformat","delim").equals("html")){


            String styleRightAlign = " class=\"sqlrunner_numeric\"";
            if(getEvalProperty("outstyle","none").equals("pretty")){
                ps.print("<style>\n");
                ps.print(".sqlrunner_resultset, .sqlrunner_resultset th, .sqlrunner_resultset td {text-align:left;}\n");
                ps.print(".sqlrunner_numeric {text-align:right;}\n");
                ps.print(".sqlrunner_resultset {border-collapse:collapse;}\n");
                ps.print(".sqlrunner_resultset, .sqlrunner_resultset th, .sqlrunner_resultset td {border: 1px solid black;}\n");
                ps.print("</style>\n");
                ps.print("<table class=\"sqlrunner_resultset\">\n");
            }else{
                styleRightAlign = "";
                ps.print("<table>\n");
            }

            // 
            // do we display header?
            //
            if(getEvalProperty("head","off").equals("on")){
                ps.print("<tr>");
                for(int i=1;i<colName.length;i++){
                    if(colIsDouble[i] || colSiz[i]==0){
                        ps.print("<th"+styleRightAlign+">"+colName[i]+"</th>");
                    }else{
                        ps.print("<th>"+colName[i]+"</th>");
                    }
                }
                ps.print("</tr>\n");
            }

            while(rs.next()){
                outcnt++;
                ps.print("<tr>");                
                for(int i=1;i<=cols;i++){
                    if(colIsDate[i]){        // if date, then format as YYYYMMDD
                        // java.sql.Date d = rs.getDate(i);
                        if(isdatetimestamp){   // oracle dates are timestamps.
                            java.sql.Timestamp d = rs.getTimestamp(i);
                            if(!rs.wasNull())
                                ps.print("<td>"+formats[i].format(d)+"</td>");
                            else ps.print("<td></td>");
                        }else{
                            java.sql.Date d = rs.getDate(i);
                            if(!rs.wasNull())
                                ps.print("<td>"+formats[i].format(d)+"</td>");
                            else ps.print("<td></td>");
                        }


                        
                    } else if(colIsTimestamp[i]){        // if date, then format as YYYYMMDDHHMMSS.SSS
                        java.sql.Timestamp d = rs.getTimestamp(i);
                        if(!rs.wasNull())
                            ps.print("<td>"+formats[i].format(d)+"</td>");
                        else ps.print("<td></td>");                        
                    } else if(colIsTime[i]){                // time column.
                        java.sql.Time d = rs.getTime(i);
                        if(!rs.wasNull())
                            ps.print("<td>"+formats[i].format(d)+"</td>");
                        else ps.print("<td></td>");                        
                    } else if(colIsDouble[i]) {             // if number has decimal point.
                        double d = rs.getDouble(i);
                        if(!rs.wasNull())
                            ps.print("<td"+styleRightAlign+">"+nf.format(d)+"</td>");
                        else ps.print("<td></td>");                        
                    } else {                        // treat everything else as string.
                        String s = rs.getString(i);
                        if(!rs.wasNull()){
                            if(colSiz[i]==0){
                                ps.print("<td"+styleRightAlign+">"+s.replace(delimChar,delimReplace).trim()+"</td>");    // trim output strings.
                            }else{
                                ps.print("<td>"+s.replace(delimChar,delimReplace).trim()+"</td>");    // trim output strings.
                            }
                        }
                        else ps.print("<td></td>");
                    }
                }
                ps.print("</tr>\n");
            }

            ps.print("</table>\n");            


        } else {
            // ???
        } 

        //
        // save the last result.
        //
        setProperty("last",outstr.trim());
        setProperty("_current_connection_laststmnt_lastrow",outstr.trim());
        setProperty("_current_connection_laststmnt_cnt",""+outcnt);

        if(getEvalProperty("console","off").equals("on")){
            System.out.println("OK ("+outcnt+" results)");
        }
    }

    /**
     * string match; similar to m/.../ in perl
     */
    public static String[] strmatch(String str,String regex){
        Matcher matcher = Pattern.compile(regex,Pattern.CASE_INSENSITIVE | Pattern.DOTALL).matcher(str);
        if(matcher.find()){
            //
            // found match! return array of all matched groups.
            //
            int n = matcher.groupCount();
            String[] s = new String[n+1];
            for(int i=0;i<=n;i++)
                s[i] = matcher.group(i);
            return s;
        }
        return null;    // no match
    }


    /**
     * process file (if f is '-' then read stdin)
     */
    public static void cmdProc(String f,PrintStream ps) throws Exception {
        
        // 
        // read input; if '-' then, use standard input, otherwise name of file.
        //
        BufferedReader in = new BufferedReader(f.equals("-") ? new InputStreamReader(System.in) : new FileReader(f), 1024);
        String line;
        String[] m;
        String runbuf = "";
        String sb = "";
        

        int lineno = 0,linenosql=0, linsize = 0;
        boolean incomment = false;      // multi-line comment.
        for(;;){
            lineno++;
            linenosql++;
            
            // display a prompt.
            if(getEvalProperty("console","off").equals("on")){
                String prompt = "";
                if(sb.trim().length() > 0 ){
                    System.out.printf(" %3d> ",linenosql);
                }else{
                    System.out.printf("SQL[%s]",timestampFormat.format(new java.util.Date(System.currentTimeMillis())));
                    if(getEvalProperty("lineno","off").equals("on"))
                        System.out.printf("("+lineno+")");
                    if(incomment)
                        System.out.print("--");
                    System.out.print("> ");
                }
            }

            // 
            // read in the next line.
            //
            line = in.readLine();
            line = rtrim(line);

            // 
            // if EOF, exit.
            //
            if(line == null)
                break;

            //
            // if we are in a multiline comment.
            //
            if(incomment){
                //
                // is comment ending on this line?
                // 
                if(line.indexOf("*/") >= 0){
                    // 
                    // cut away the comment. get out of a multiline comment.
                    //
                    line = line.substring( line.indexOf("*/") + 2);
                    incomment = false;
                }else{
                    //
                    // ignore line; we're in a multi-line comment.
                    //
                    continue;
                }
            }


            {
                // clumsy finite automata; need to properly handle quotes 
                // (and potential escape characters within quotes).
                StringBuffer s = new StringBuffer();
                String state = "s";
                
                String[] fa_states = {
                    "s:'", "q::1", // go into quote
                    "q:'", "s::1", // go out of quote
                    "q:\\", "sq::1",    // escape next char in quote
                    "sq:", "q::1",      // pass on escaped char
                    "q:",   "q::1",

                    "s:\"", "qq::1", // go into dquote
                    "qq:\"", "s::1", // go out of dquote
                    "qq:\\", "sqq::1",    // escape next char in dquote
                    "sqq:", "qq::1",      // pass on escaped char
                    "qq:",  "qq::1",

                    "s:/", "sl::0",
                    "sl:/", "sl2::0",    // double slash comment
                    "sl:",  "s:/:1",    // not a comment.

                    "sl:*", "sls::0",    // slash star comment
                    "sls:*", "sls2::0",    // end slash star comment
                    "sls2:", "sls::0",    
                    "sls2:/", "s::0",       // end slash star comment

                    "s:-", "sd::0",
                    "sd:-", "sd2::0",    // double dash comment
                    "sd:",  "s:-:1",    // not a comment.

                    "s:#", "sc::0",      // comments

                    "s:",   "s::1"     // pass on everything else
                };
                // create map
                TreeMap<String,String> st = new TreeMap<String,String>();
                for(int i=0;i<fa_states.length;i+=2){
                    st.put(fa_states[i],fa_states[i+1]);
                }
                // finite automata driver
                for(int i=0;i<line.length();i++){
                    char ch = line.charAt(i);
                    String next = st.get(state+':'+ch);
                    if(next == null)
                        next = st.get(state+':');
                    if(next != null){
                        String[] nextc = next.split(":");
                        state = nextc[0];
                        s.append(nextc[1]);
                        if(nextc[2].equals("1")) s.append(ch);
                    }
                }
                {
                    String next = st.get(state+':');
                    if(next != null){
                        String[] nextc = next.split(":");
                        state = nextc[0];
                        s.append(nextc[1]);
                    }
                }

                line = s.toString();
                if(state.equals("sls") || state.equals("sls2")){
                    incomment = true;
                }

            }

            // size of line (is there anything there?)
            linsize = line.length();

            // add support for "fake" sqlrunner syntax :-)
            if( getEvalProperty("nysesqlrunner","off").equals("on") ){
                String orig_line = line;
                // support for weird syntax.
                line = Pattern.compile("--@@\\s*IMPORT\\s*\\('classpath:(.*?)'\\);",
                    Pattern.CASE_INSENSITIVE | Pattern.DOTALL).matcher(line).replaceAll("@$1");
                line = Pattern.compile("--@@\\s*SET\\s*(.*)",
                    Pattern.CASE_INSENSITIVE | Pattern.DOTALL).matcher(line).replaceAll("def $1");
                line = Pattern.compile("--@@\\s*ignore_sql_error\\s*=\\s*on\\s*;",
                    Pattern.CASE_INSENSITIVE | Pattern.DOTALL).matcher(line).replaceAll("def  errors='off';");
                line = Pattern.compile("--@@\\s*ignore_sql_error\\s*=\\s*off\\s*;",
                    Pattern.CASE_INSENSITIVE | Pattern.DOTALL).matcher(line).replaceAll("def errors='on';");
            }
            
            line = rtrim(line);

            //
            // user wants to run current buffer.
            //
            if(line.equals( getEvalProperty("gocmd","/") )){
                //
                // runbuf becomes current buffer (if current buffer isn't empty).
                //    
                if(sb.trim().length() > 0)
                    runbuf = sb;
                sb = "";
                linenosql = 0;
                try{
                    // run sql (spool to ps)
                    sqlRun(runbuf,ps);
                }catch(Exception e){
                    // ignore errors on drops.
                    if(getEvalProperty("errors","on").equals("on") && strmatch(evalString(runbuf),"^\\s*DROP\\s") == null){
                        if(getEvalProperty("console","off").equals("on")){
                            // don't bomb out of console mode.
                            System.out.println("ERROR: "+e.getMessage());
                        }else{
                            throw new Exception(f+"("+lineno+"): "+e.getMessage(),e);
                        }
                    }
                }
            }else if(linsize == 0){
                //
                // if empty line, setup run buffer to current buffer (if current buffer is not empty).
                //
                // if(sb.trim().length() > 0)
                //    runbuf = sb;
                //sb = "";
                //linenosql = 0;
            }else if(sb.trim().length() > 0){
                //
                // if current buffer isn't empty, then add line to current buffer.
                //

                //
                // if there's a ';' end of line, remove it.
                //
                if(line.endsWith(getEvalProperty("cmdseparator",";"))){
                    line = line.substring(0,line.length()-1).trim();
                    if(line.length() > 0)
                        sb += line + "\n";
                    
                    //
                    // TODO: refactor the code.
                    // runbuf becomes current buffer (if current buffer isn't empty).
                    //    
                    if(sb.trim().length() > 0)
                        runbuf = sb;
                    sb = "";
                    linenosql = 0;
                    try{
                        // run sql (spool to ps)
                        sqlRun(runbuf,ps);
                        runbuf="";
                    }catch(Exception e){
                        // ignore errors on drops.
                        if(getEvalProperty("errors","on").equals("on") && strmatch(evalString(runbuf),"^\\s*DROP\\s") == null){
                            if(getEvalProperty("console","off").equals("on")){
                                // don't bomb out of console mode.
                                System.out.println("ERROR: "+e.getMessage());
                            }else{
                                throw new Exception(f+"("+lineno+"): "+e.getMessage(),e);
                            }
                        }
                    }

                }else{
                    if(line.length() > 0)
                        sb += line + "\n";
                }
            }else if(strmatch(line,"^\\s*set\\s+") != null && getEvalProperty("set","off").equals("on")){
                // 
                // handle the "set " command.
                // this can be:  set blah glah on spool env log off
                //  (ie: consistent with sqlplus, this sets glah blah to "on" and spool env log to "off")
                //

                //
                // if there's a ';' end of line, remove it.
                //            
                if(line.endsWith(getEvalProperty("cmdseparator",";")))
                    line = line.substring(0,line.length()-1).trim();                
                
                Vector<String> v = new Vector<String>();
                for(String s : line.split("\\s+")){
                    if(s.equalsIgnoreCase("on") || s.equalsIgnoreCase("off")){
                        for(String t : v)
                            setProperty(t,s.toLowerCase());
                        v.removeAllElements();
                    }else if(s.length() > 0){
                        v.add(s);
                    }
                }
                if(v.size() > 0){    // if set doesn't have an "on" or "off"
                    if(getEvalProperty("console","off").equals("on")){
                        // don't bomb out of console mode.
                        System.out.println("ERROR: Improperly formatted 'set' command");
                    }else{
                        throw new IllegalStateException(f+"("+lineno+"): Improperly formatted 'set' command.");
                    }
                }
                sb = "";
                linenosql = 0;
            }else if(strmatch(line,"^\\s*(unset|undef(ine)?)\\s+") != null && getEvalProperty("set","off").equals("on")){
                // don't think anyone would care, but default is to mask undefine command.
                // 
                // handle the "unset " command.
                // this can be:  unset blah glah on spool env log

                // if there's a ';' end of line, remove it.
                if(line.endsWith(getEvalProperty("cmdseparator",";")))
                    line = line.substring(0,line.length()-1).trim();
                
                Vector<String> v = new Vector<String>();
                for(String s : line.split("\\s+")){
                    if(s.length() > 0)
                        clearProperty(s);
                }
                sb = "";
                linenosql = 0;
            }else if(
                ((m = strmatch(line,"^\\s*def(ine)?\\s+(\\w+)\\s*=\\s*'([^']+)'\\s*;?\\s*$")) != null) ||
                ((m = strmatch(line,"^\\s*def(ine)?\\s+(\\w+)\\s*=\\s*[\"]?([^\"]+)[\"]?\\s*;?\\s*$")) != null)
                ){
                // 
                // handle the 'def ' command, ie:
                //    def tdate='20071031';
                // 
                setProperty(m[2],evalString(m[3].trim()));
                sb = "";
                linenosql = 0;
            }else if(
                ((m = strmatch(line,"^\\s*def(ine)?q\\s+(\\w+)\\s*=\\s*'([^']+)'\\s*;?\\s*$")) != null) ||
                ((m = strmatch(line,"^\\s*def(ine)?q\\s+(\\w+)\\s*=\\s*[\"]?([^\"]+)[\"]?\\s*;?\\s*$")) != null) 
                ){
                // 
                // handle the 'defq ' command, ie:
                //    defq tdate='&&year.&&month.&&day';
                // // will set value "as is"; meaning it can be evaluated later. 
                setProperty(m[2],m[3].trim());
                sb = "";
                linenosql = 0;
            }else if(
                ((m = strmatch(line,"^\\s*echo\\s*'([^']+)'\\s*;?\\s*$")) != null) ||
                ((m = strmatch(line,"^\\s*echo\\s*[\"]?([^\"]+)[\"]?\\s*;?\\s*$")) != null) 
                ){
                //
                // handle the 'echo ' command, ie:
                //    echo "hello world"
                // 
                if(ps != null)
                    ps.println(evalString(m[1]));
                sb = "";
                linenosql = 0;
            }else if(
                ((m = strmatch(line,"^\\s*spool\\s+'([^']+)'\\s*;?\\s*$")) != null) || 
                ((m = strmatch(line,"^\\s*spool\\s+[\"]?([^\"]+)[\"]?\\s*;?\\s*$")) != null) 
                ){
                //
                // setup spooling to a file (csv)
                //
                setProperty("spool",evalString(m[1].trim()));
                sb = "";
                linenosql = 0;

                try{
                    // if ps is not stdout, close it.
                    if(ps != null && ps != System.out)
                        ps.close();

                    //
                    // setup spooling to a file (if spool isn't "off")
                    if(getEvalProperty("spool","stdout").equals("stdout")){
                        ps = System.out;
                    }else if(getEvalProperty("spool","stdout").equals("off") || getEvalProperty("spool","stdout").equals("null")){
                        ps = null;
                    }else {
                        ps = new PrintStream( 
                            getEvalProperty("spool","").endsWith(".gz") ?
                            new GZIPOutputStream( new FileOutputStream( System.getProperty("spool") ) ) :  
                            new FileOutputStream( System.getProperty("spool") )
                        );
                    }
                }catch(Exception e){
                            if(getProperty("console","off").equals("on")){
                                // don't bomb out of console mode.
                                System.out.println("ERROR: "+e.getMessage());
                            }else{
                                throw new Exception(f+"("+lineno+"): "+e.getMessage(),e);
                            }
                    
                }
            }else if(
                ((m = strmatch(line,"^\\s*@\\s*'([^']+)'\\s*;?\\s*$")) != null) || 
                ((m = strmatch(line,"^\\s*@\\s*[\"]?([^\"]+)[\"]?\\s*;?\\s*$")) != null)
                ){
                //
                // recursively process a file
                //
                line = evalString(m[1]);
                sb = "";
                linenosql = 0;

                // check if we're already processing this file.
                if(processingFiles.contains(line)){
                    throw new IllegalStateException("cannot recursively include: "+line);
                }

                try {
                    // push file to stack
                    processingFiles.push(line);
                    
                    // if .properties file, then load it into properties.
                    if(line.toLowerCase().endsWith(".properties")){
                        props.load(new FileInputStream(line));
                    }else{
                        setProperty("_"+line+"_startmillis",""+System.currentTimeMillis());
                        cmdProc(line,ps);
                    }
                }catch(Exception e){
                    if(getEvalProperty("console","off").equals("on")){
                        // don't bomb out of console mode.
                        System.out.println("ERROR: "+e.getMessage());
                    }else{
                        throw new Exception(f+"("+lineno+"): "+e.getMessage(),e);
                    }
                }finally{
                    setProperty("_"+line+"_endmillis",""+System.currentTimeMillis());

                    if(getEvalProperty("log","off").equals("on")){
                        long millis = 
                            Long.parseLong( getProperty("_"+line+"_startmillis","0") ) - 
                            Long.parseLong( getProperty("_"+line+"_endmillis","0"));
                        System.out.printf("\n-- File %s completed in: %.2f minutes.\n",line,(millis/1000.0)/60.0);
                    }
                    // remove file from stack.
                    processingFiles.pop();
                }

            }else if(strmatch(line,"^\\s*exit\\s*;?\\s*$") != null){

                setProperty("_TOTAL_endmillis",""+System.currentTimeMillis());
                if(getEvalProperty("log","off").equals("on")){
                    long millis = 
                        Long.parseLong( getProperty("_TOTAL_endmillis","0") ) - 
                        Long.parseLong( getProperty("_TOTAL_startmillis","0"));
                    System.out.printf("\n-- Total SQLRunner execution time: %.2f minutes.\n",(millis/1000.0)/60.0);
                }


                // close spool file if not stdout.
                if(ps != null && ps != System.out)
                    ps.close();

                //
                // exits the program.
                //
                System.exit(0);
            }else if(strmatch(line,"^\\s*quit\\s*;?\\s*$") != null){
                //
                // ends processing of current file.
                //
                break;
            }else if(line.length() > 0){
                //
                // if not one of the commands, then starts new buffer.
                //

                //
                // if there's a ';' end of line, remove it.
                //
                if(line.endsWith(getEvalProperty("cmdseparator",";"))){
                    line = line.substring(0,line.length()-1).trim();

                    if(line.length() > 0)
                        sb += line + "\n";
                    
                    //
                    // TODO: refactor the code.
                    // runbuf becomes current buffer (if current buffer isn't empty).
                    //    
                    if(sb.trim().length() > 0)
                        runbuf = sb;
                    sb = "";
                    linenosql = 0;
                    try{
                        // run sql (spool to ps)
                        sqlRun(runbuf,ps);
                        runbuf="";
                    }catch(Exception e){
                        // ignore errors on drops.
                        if(getEvalProperty("errors","on").equals("on") && strmatch(evalString(runbuf),"^\\s*DROP\\s") == null){
                            if(getEvalProperty("console","off").equals("on")){
                                // don't bomb out of console mode.
                                System.out.println("ERROR: "+e.getMessage());
                            }else{
                                throw new Exception(f+"("+lineno+"): "+e.getMessage(),e);
                            }
                        }
                    }

                }else{
                    sb += line + "\n";
                }
            }
        }


        // if hit EOF, with no end of line, then assume that's a SQL statement.
        // ie: echo "select * blah where glah=323" | sqlrunner db=glah - 
        if(sb.trim().length() > 0){
            runbuf = sb;
            sb = "";
            linenosql = 0;
            try{
                // run sql (spool to ps)
                sqlRun(runbuf,ps);
                runbuf="";
            }catch(Exception e){
                // ignore errors on drops.
                if(getEvalProperty("errors","on").equals("on") && strmatch(evalString(runbuf),"^\\s*DROP\\s") == null){
                    if(getEvalProperty("console","off").equals("on")){
                        // don't bomb out of console mode.
                        System.out.println("ERROR: "+e.getMessage());
                    }else{
                        throw new Exception(f+"("+lineno+"): "+e.getMessage(),e);
                    }
                }
            }
        }

        // close spool file if not stdout
        if(ps != null && ps != System.out)
            ps.close();
    }

    /**
     * loop through command line args... and run stuff that appears there.
     */
    public static void main(String[] args) throws Exception {

        setProperty("_TOTAL_startmillis",""+System.currentTimeMillis());

        // defaults that user can change.
        setProperty("env_default","off");
        setProperty("set_default","off");
        setProperty("log_default","off");
        setProperty("console_default","off");
        setProperty("errors_default","on");
        setProperty("pretend_default","off");
        setProperty("debug_default","off");
        setProperty("spool_default","stdout");
        setProperty("cmdseparator_default",";");
        setProperty("gocmd_default","/");
        setProperty("delimReplace_default"," ");
        setProperty("delim_default",",");
        setProperty("textdelim_default"," ");
        setProperty("linesep_default","\n");
        setProperty("outformat_default","delim");
        setProperty("db_default","db");
        setProperty("version","unknown");
        setProperty("amp","&");
        setProperty("tab","\t");
        setProperty("nullval","");
        // 
        setProperty("tns","&&amp.&&amp.&&db._tns");
        setProperty("url","&&amp.&&amp.&&db._url");
        setProperty("driver","&&amp.&&amp.&&db._driver");
        setProperty("user","&&amp.&&amp.&&db._user");
        setProperty("pass","&&amp.&&amp.&&db._pass");
        setProperty("dbname","&&amp.&&amp.&&db._dbname");
        setProperty("host","&&amp.&&amp.&&db._host");
        setProperty("port","&&amp.&&amp.&&db._port");

        //
        // use environment as default params
        // 
        Map<String, String> env = System.getenv();
        for (String envName : env.keySet()) {
            setProperty(envName,env.get(envName));
        }

        for(String cmd : args){
            //
            // if someone types name=value in command line, then set it as environment.
            // 
            String[] nv = cmd.split("=",2);
            setProperty(nv[0],(nv.length > 1 ? nv[1] : "on"));
            
            //
            // if .properties, then load it into system properties. 
            // else process it as input file.
            //
            if(cmd.endsWith(".properties")){      // this is a properties file.
                props.load(new FileInputStream(cmd));
            }else if(cmd.toLowerCase().endsWith(".sql") || cmd.equals("-")){  // run this sql code
                try {
                    setProperty("_"+cmd+"_startmillis",""+System.currentTimeMillis());
                    cmdProc(cmd,System.out);
                } finally { 
                    setProperty("_"+cmd+"_endmillis",""+System.currentTimeMillis());

                    if(getEvalProperty("log","off").equals("on")){
                        long millis = 
                            Long.parseLong( getProperty("_"+cmd+"_endmillis","0") ) - 
                            Long.parseLong( getProperty("_"+cmd+"_startmillis","0"));
                        System.out.printf("\n-- File %s completed in: %.2f minutes.\n",cmd,(millis/1000.0)/60.0);
                    }
                }
            }
        }

        setProperty("_TOTAL_endmillis",""+System.currentTimeMillis());
        if(getEvalProperty("log","off").equals("on")){
            long millis = 
                Long.parseLong( getProperty("_TOTAL_endmillis","0") ) - 
                Long.parseLong( getProperty("_TOTAL_startmillis","0"));
            System.out.printf("\n-- Total SQLRunner execution time: %.2f minutes.\n",(millis/1000.0)/60.0);
        }
        System.exit(0);   // workaround for Hive JDBC driver bug; it leaves threads around.
    }
}

