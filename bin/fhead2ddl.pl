#!/usr/bin/perl

# table ddl grabber

# Mon Mar 17 05:19:14 EDT 2008

# Copyright (c) 2005, Alex Sverdlov

# This work is free software; you can redistribute it and/or modify it
# under the terms of the GNU General Public License as published by 
# the Free Software Foundation, version 2, or higher.


use strict;
use warnings;

my (%args);
map {my ($n,$v)=split/=/; $args{$n}= defined($v) ? $v:1; } @ARGV;

if($args{help} || $args{'-help'} || $args{'--help'}){
    print "Move Data Between Databases\n";
    print qq|
    Commands include:
        help: (this screen)
    |;
    exit;
}

my $input = <STDIN>;

my $args = join(" ",map { "$_='$args{$_}'"} keys %args);

$args{dateformat} = 'YYYYMMDD' unless $args{dateformat};
$args{timeformat} = 'HH24:MI:SS' unless $args{timeformat};
$args{timestampformat} = $args{dateformat}.' '.$args{timeformat} unless $args{timestampformat};

#my $cmd = qq(echo 'select * from $args{table} where 0=1'| sqlrunner fhead $args - 2>/dev/null);
my @cols = map { 
    s/^\s+|\s+$//sgi; 
    my $r={}; 
    @{$r}{qw(name type size pres scale origtype null)}=split/~/;
    if($r->{type} =~ m/varchar/i && $r->{size} == 0){
        @{$r}{qw(nztype oratype oractltype hivetype hive13type)} = qw(VARCHAR VARCHAR2 CHAR STRING STRING);
    }elsif($r->{type} =~ m/varchar/i){
        @{$r}{qw(nztype oratype oractltype hivetype hive13type)} = 
            ("VARCHAR($r->{size})","VARCHAR2($r->{size})",$r->{size} > 100 ? "CHAR($r->{size})" : "CHAR",  "STRING", "VARCHAR($r->{size})");
    }elsif($r->{type} =~ m/char/i && $r->{size} == 0){
        @{$r}{qw(nztype oratype oractltype hivetype hive13type)} = qw(CHAR CHAR CHAR STRING STRING);
    }elsif($r->{type} =~ m/char/i){
        @{$r}{qw(nztype oratype oractltype hivetype hive13type)} = 
            ("CHAR($r->{size})","CHAR($r->{size})",$r->{size} > 100 ? "CHAR($r->{size})" : "CHAR","STRING", "STRING");
    }elsif($r->{type} =~ m/number/i && $r->{pres} > 0 && $r->{scale} > 0){
        @{$r}{qw(nztype oratype oractltype hivetype hive13type)} = 
            ("NUMERIC($r->{pres},$r->{scale})",
            "NUMBER($r->{pres},$r->{scale})","DECIMAL EXTERNAL","DOUBLE","DECIMAL($r->{pres},$r->{scale})");
    }elsif($r->{type} =~ m/number/i && $r->{pres} > 0){
        if($r->{pres} == 10){
            @{$r}{qw(nztype oratype oractltype hivetype hive13type)} = 
                ("INT4",
                    "NUMBER($r->{pres})","INTEGER EXTERNAL","INT", "INT");
        }elsif($r->{pres} == 19){
            @{$r}{qw(nztype oratype oractltype hivetype hive13type)} = 
                ("INT8",
                    "NUMBER($r->{pres})","INTEGER EXTERNAL","INT","BIGINT");
        }else{
            @{$r}{qw(nztype oratype oractltype hivetype hive13type)} = 
                ("NUMERIC($r->{pres})",
                    "NUMBER($r->{pres})","INTEGER EXTERNAL","INT","INT");
        }
    }elsif($r->{type} =~ m/number/i){
        @{$r}{qw(nztype oratype oractltype hivetype hive13type)} = 
            ("DOUBLE PRECISION",
            "NUMBER","DECIMAL EXTERNAL","DOUBLE","DOUBLE");
    }elsif($r->{type} =~ m/date/i){
        @{$r}{qw(nztype oratype oractltype hivetype hive13type)} = 
            ("DATE","DATE","DATE 'YYYYMMDD'","STRING","DATE");
    }elsif($r->{type} =~ m/timestamp/i){
        my $name = $r->{name};
        @{$r}{qw(nztype oratype oractltype hivetype hive13type)} = 
            ("TIMESTAMP","TIMESTAMP",qq{TIMESTAMP "to_timestamp(:$name, '$args{timestampformat}')" },"STRING","TIMESTAMP");
    }elsif($r->{type} =~ m/time/i){
        my $name = $r->{name};
        @{$r}{qw(nztype oratype oractltype hivetype hive13type)} = 
            ("TIME","DATE",qq{DATE "to_date(:$name, '$args{timeformat}')"},"STRING","STRING"); 
    }else{
        @{$r}{qw(nztype oratype oractltype hivetype hive13type)} = 
            ($r->{type},$r->{type},"CHAR","STRING","STRING");
    }
    $r
} split /,/,$input;

$args{to_table} = $args{table} unless $args{to_table};

my $ddl2 = "";
if($args{type} =~ m/nz|gp|pg/i){
    $ddl2 .= join(",",map {$_->{name}.' '.$_->{nztype}} @cols);
}elsif($args{type} =~ m/hive13/i){
    $ddl2 .= join(",",map {$_->{name}.' '.$_->{hive13type}} @cols);
}elsif($args{type} =~ m/hive/i){
    $ddl2 .= join(",",map {$_->{name}.' '.$_->{hivetype}} @cols);
}else{
    $ddl2 .= join(",",map {$_->{name}.' '.$_->{oratype}} @cols);
}
my $ddl = "CREATE TABLE $args{to_table} (". $ddl2.") ";
$ddl .= " distribute on random" if $args{type} =~ m/nz/i;
$ddl .= " distributed randomly" if $args{type} =~ m/gp/i;


if($args{nocreate}){
    print $ddl2."\n";   # leave create table stuff to the user.
}else{
    print "$ddl\n";
}

