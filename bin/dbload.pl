#!/usr/bin/perl

# generic database loader


# Note that this program exposes username/passwords to environment, 
# e.g. someone doing: ps -ef |grep ... may get your database connection info.

# Copyright (c) 2005, Alex Sverdlov

# This work is free software; you can redistribute it and/or modify it
# under the terms of the GNU General Public License as published by 
# the Free Software Foundation, version 2, or higher.


use strict;
use warnings;

my (%args);
map {my ($n,$v)=split/=/,$_,2; $args{$n}= defined($v) ? $v:1; } @ARGV;

if($args{help} || $args{'-help'} || $args{'--help'}){
    print "read data from stdin, and load it into db\n";
    print qq|
    Commands include:
        help: (this screen)
    |;
    exit;
}

die "db is undefined\n" unless defined($args{db});
die "table is undefined\n" unless defined($args{table});

for my $e (qw(db dbname url pass user)){
    $ENV{$e} = $args{$e} if defined $args{$e};
}

$args{tmp}=$ENV{TMP} unless defined($args{tmp});
$args{tmp}=$ENV{TEMP} unless defined($args{tmp});
$args{tmp}="/tmp" unless defined($args{tmp});

unless(defined $args{fhead}){
    $args{fhead}=`echo "select * from $args{table} where 1=0"|sqlrunner db=$args{db} fhead - 2>/dev/null`;
}
die "fhead is undefined\n" unless length($args{fhead}) > 0;

$args{delim} = "," unless defined $args{delim};

# figure out which db we're loading into.
my $info=`echo "info"|sqlrunner db=$args{db} - 2>/dev/null` . 
    `echo "select version()"|sqlrunner db=$args{db} - 2>/dev/null`;

if($info =~ m/oracle/is){
    
    # get oracle user/pass@TNSNAME
    my $oratns=`echo "echo &&$args{db}_user/&&$args{db}_pass@&&$args{db}_tns"|sqlrunner db=$args{db} -`;
    $oratns=~s/^\s+|\s+$//sgi;

    my $clauses = "";
    for my $v (qw(delim trunc dateformat timeformat timestampformat direct)){
        $clauses .= qq{$v="$args{$v}" } if defined($args{$v});
    }

    print qq(echo "$args{fhead}" |fhead2ctl.pl table="$args{table}" file="-" $clauses par="$args{tmp}/dbload.$args{db}.$args{table}.$$" userid="$oratns" > "$args{tmp}/dbload.$args{db}.$args{table}.$$.ctl"\n\n) if $args{log};

    # gen ora ctl and par file.
    `echo "$args{fhead}" |fhead2ctl.pl table="$args{table}" file="-" $clauses par="$args{tmp}/dbload.$args{db}.$args{table}.$$" userid="$oratns" > "$args{tmp}/dbload.$args{db}.$args{table}.$$.ctl"`;

    print qq(echo "drop table $args{table}"|sqlrunner db=$args{db} - 2>/dev/null\n\n) if $args{log};

    # drop table if specified.
    `echo "drop table $args{table}"|sqlrunner db=$args{db} - 2>/dev/null` if $args{drop};

    print qq(echo "$args{fhead}"|fhead2ddl.pl table="$args{table}" type=ora |sqlrunner db=$args{db} - 2>/dev/null\n\n) if $args{log};

    # attempt creating the destination table
    `echo "$args{fhead}"|fhead2ddl.pl table="$args{table}" type=ora |sqlrunner db=$args{db} - 2>/dev/null`;

    print qq(sqlldr parfile="$args{tmp}/dbload.$args{db}.$args{table}.$$.par"\n\n)  if $args{log};

    # will read stdin for data.
    `sqlldr parfile="$args{tmp}/dbload.$args{db}.$args{table}.$$.par"`;

    die qq{sqlldr failed. 
ctl=$args{tmp}/dbload.$args{db}.$args{table}.$$.ctl 
par=$args{tmp}/dbload.$args{db}.$args{table}.$$.par 
log=$args{tmp}/dbload.$args{db}.$args{table}.$$.log 
bad=$args{tmp}/dbload.$args{db}.$args{table}.$$.bad

Note: If load failed due to subseconds in timestamps, consider setting: 
    timeformat="HH24:MI:SS.FF3" (for milliseconds)
    timeformat="HH24:MI:SS.FF6" (for microseconds) 
    \n} if $? != 0;

    unlink "$args{tmp}/dbload.$args{db}.$args{table}.$$.ctl", "$args{tmp}/dbload.$args{db}.$args{table}.$$.par", "$args{tmp}/dbload.$args{db}.$args{table}.$$.bad", "$args{tmp}/dbload.$args{db}.$args{table}.$$.log" unless $args{keeplog};

}elsif($info =~ m/netezza/is){
    
    # get connection settings.
    $args{host}=`echo "echo &&$args{db}_host"|sqlrunner db=$args{db} -` unless defined $args{host};
    $args{dbname}=`echo "echo &&$args{db}_dbname"|sqlrunner db=$args{db} -` unless defined $args{dbname};
    $args{user}=`echo "echo &&$args{db}_user"|sqlrunner db=$args{db} -` unless defined $args{user};
    $args{pass}=`echo "echo &&$args{db}_pass"|sqlrunner db=$args{db} -` unless defined $args{pass};
    
    map { $args{$_}=~s/^\s+|\s+$//sgi } qw(host dbname user pass);
    $ENV{NZ_PASSWORD} = $args{pass};
    $ENV{NZ_USER} = $args{user};

    # drop table if specified.
    `echo "drop table $args{table}"|sqlrunner db=$args{db} - 2>/dev/null` if $args{drop};

    # attempt creating the destination table
    `echo "$args{fhead}"|fhead2ddl.pl table="$args{table}" type=nz |sqlrunner db=$args{db} - 2>/dev/null`;

    $args{datestyle} = 'ymd' unless defined $args{datestyle};
    $args{datedelim} = '' unless defined $args{datedelim};
    $args{timestyle} = '24hour' unless defined $args{timestyle};    # 12hour ?
    $args{timedelim} = ':' unless defined $args{timedelim};

    my $cmd=qq{nzload -host $args{host} -db $args{dbname} -t $args{table} -delim '$args{delim}' -datestyle '$args{datestyle}' -datedelim '$args{datedelim}' -timestyle '$args{timestyle}' -timedelim '$args{timedelim}' -nullvalue '' -maxerrors 100 -lf "$args{tmp}/dbload.$args{db}.$args{table}.$$.log" -bf '$args{tmp}/dbload.$args{db}.$args{table}.$$.bad' -fillrecord };
    `$cmd`;

    die "nzload failed ($cmd)\n" if $? != 0;

    unlink "$args{tmp}/dbload.$args{db}.$args{table}.$$.bad", "$args{tmp}/dbload.$args{db}.$args{table}.$$.log" unless $args{keeplog};


}elsif($info =~ m/greenplum/is){

    # get connection settings.
    $args{host}=`echo "echo &&$args{db}_host"|sqlrunner db=$args{db} -` unless defined $args{host};
    $args{dbname}=`echo "echo &&$args{db}_dbname"|sqlrunner db=$args{db} -` unless defined $args{dbname};
    $args{user}=`echo "echo &&$args{db}_user"|sqlrunner db=$args{db} -` unless defined $args{user};
    $args{pass}=`echo "echo &&$args{db}_pass"|sqlrunner db=$args{db} -` unless defined $args{pass};
    
    map { $args{$_}=~s/^\s+|\s+$//sgi } qw(host dbname user pass);
    $ENV{PGPASSWORD} = $args{pass};
    $ENV{PGUSER} = $args{user};
    $ENV{PGDATABASE} = $args{dbname};
    $ENV{PGHOST} = $args{host};

    # drop table if specified.
    `echo "drop table $args{table}"|sqlrunner db=$args{db} - 2>/dev/null` if $args{drop};

    # attempt creating the destination table
    `echo "$args{fhead}"|fhead2ddl.pl table="$args{table}" type=gp |sqlrunner db=$args{db} - 2>/dev/null`;

    $args{datestyle} = 'ymd' unless defined $args{datestyle};
    $args{datedelim} = '' unless defined $args{datedelim};
    $args{timestyle} = '24hour' unless defined $args{timestyle};    # 12hour ?
    $args{timedelim} = ':' unless defined $args{timedelim};

    my $cmd=qq{psql -c"\\copy $args{table} from STDIN with delimiter '$args{delim}' null as '' FILL MISSING FIELDS"};
    
    `$cmd`;

    die qq{psql failed ($cmd)    
If load filed due to non-UTF8 characters, perhaps you need to 
set environment variable: PGCLIENTENCODING=LATIN1
    \n} if $? != 0;

    # we don't have a log for this yet...
    unlink "$args{tmp}/dbload.$args{db}.$args{table}.$$.bad", "$args{tmp}/dbload.$args{db}.$args{table}.$$.log" unless $args{keeplog};

}elsif($info =~ m/postgres/is){

    # get connection settings.
    $args{host}=`echo "echo &&$args{db}_host"|sqlrunner db=$args{db} -` unless defined $args{host};
    $args{dbname}=`echo "echo &&$args{db}_dbname"|sqlrunner db=$args{db} -` unless defined $args{dbname};
    $args{user}=`echo "echo &&$args{db}_user"|sqlrunner db=$args{db} -` unless defined $args{user};
    $args{pass}=`echo "echo &&$args{db}_pass"|sqlrunner db=$args{db} -` unless defined $args{pass};
    
    map { $args{$_}=~s/^\s+|\s+$//sgi } qw(host dbname user pass);
    $ENV{PGPASSWORD} = $args{pass};
    $ENV{PGUSER} = $args{user};
    $ENV{PGDATABASE} = $args{dbname};
    $ENV{PGHOST} = $args{host};

    # drop table if specified.
    `echo "drop table $args{table}"|sqlrunner db=$args{db} - 2>/dev/null` if $args{drop};

    # attempt creating the destination table
    `echo "$args{fhead}"|fhead2ddl.pl table="$args{table}" type=pg |sqlrunner db=$args{db} - 2>/dev/null`;

    $args{datestyle} = 'ymd' unless defined $args{datestyle};
    $args{datedelim} = '' unless defined $args{datedelim};
    $args{timestyle} = '24hour' unless defined $args{timestyle};    # 12hour ?
    $args{timedelim} = ':' unless defined $args{timedelim};

    my $cmd=qq{psql -c"\\copy $args{table} from STDIN with delimiter '$args{delim}' null as '' "};
    
    `$cmd`;

    die qq{psql failed ($cmd)
If load filed due to non-UTF characters, perhaps you need to 
set environment variable: PGCLIENTENCODING=LATIN1
    \n} if $? != 0;

    # we don't have a log for this yet...
    unlink "$args{tmp}/dbload.$args{db}.$args{table}.$$.bad", "$args{tmp}/dbload.$args{db}.$args{table}.$$.log" unless $args{keeplog};

}else{
    # TODO: add other databases.
    die "invalid db=$args{db}\n";
}

