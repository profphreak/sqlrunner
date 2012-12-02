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
        @{$r}{qw(nztype oratype oractltype hivetype)} = qw(VARCHAR VARCHAR2 CHAR STRING);
    }elsif($r->{type} =~ m/varchar/i){
        @{$r}{qw(nztype oratype oractltype hivetype)} = 
            ("VARCHAR($r->{size})","VARCHAR2($r->{size})",$r->{size} > 100 ? "CHAR($r->{size})" : "CHAR",  "STRING");
    }elsif($r->{type} =~ m/char/i && $r->{size} == 0){
        @{$r}{qw(nztype oratype oractltype hivetype)} = qw(CHAR CHAR CHAR STRING);
    }elsif($r->{type} =~ m/char/i){
        @{$r}{qw(nztype oratype oractltype hivetype)} = 
            ("CHAR($r->{size})","CHAR($r->{size})",$r->{size} > 100 ? "CHAR($r->{size})" : "CHAR","STRING");
    }elsif($r->{type} =~ m/number/i && $r->{pres} > 0 && $r->{scale} > 0){
        @{$r}{qw(nztype oratype oractltype hivetype)} = 
            ("NUMERIC($r->{pres},$r->{scale})",
            "NUMBER($r->{pres},$r->{scale})","DECIMAL EXTERNAL","DOUBLE");
    }elsif($r->{type} =~ m/number/i && $r->{pres} > 0){
        if($r->{pres} == 10){
            @{$r}{qw(nztype oratype oractltype hivetype)} = 
                ("INT4",
                    "NUMBER($r->{pres})","INTEGER EXTERNAL","INT");
        }elsif($r->{pres} == 19){
            @{$r}{qw(nztype oratype oractltype hivetype)} = 
                ("INT8",
                    "NUMBER($r->{pres})","INTEGER EXTERNAL","INT");
        }else{
            @{$r}{qw(nztype oratype oractltype hivetype)} = 
                ("NUMERIC($r->{pres})",
                    "NUMBER($r->{pres})","INTEGER EXTERNAL","INT");
        }
    }elsif($r->{type} =~ m/number/i){
        @{$r}{qw(nztype oratype oractltype hivetype)} = 
            ("DOUBLE PRECISION",
            "NUMBER","DECIMAL EXTERNAL","DOUBLE");
    }elsif($r->{type} =~ m/date/i){
        @{$r}{qw(nztype oratype oractltype hivetype)} = 
            ("DATE","DATE","DATE 'YYYYMMDD'","STRING");
    }elsif($r->{type} =~ m/timestamp/i){
        my $name = $r->{name};
        @{$r}{qw(nztype oratype oractltype hivetype)} = 
            ("TIMESTAMP","TIMESTAMP",qq{TIMESTAMP "to_timestamp(:$name, '$args{timestampformat}')" },"STRING");
    }elsif($r->{type} =~ m/time/i){
        my $name = $r->{name};
        @{$r}{qw(nztype oratype oractltype hivetype)} = 
            ("TIME","DATE",qq{DATE "to_date(:$name, '$args{timeformat}')"},"STRING"); 
    }else{
        @{$r}{qw(nztype oratype oractltype hivetype)} = 
            ($r->{type},$r->{type},"CHAR","STRING");
    }
    $r
} split /,/,$input;

$args{to_table} = $args{table} unless $args{to_table};

my $ddl = "CREATE TABLE $args{to_table} (";
if($args{type} =~ m/nz|gp|pg/i){
    $ddl .= join(",",map {$_->{name}.' '.$_->{nztype}} @cols).") ";
}elsif($args{type} =~ m/hive/i){
    $ddl .= join(",",map {$_->{name}.' '.$_->{hivetype}} @cols).") ";
}else{
    $ddl .= join(",",map {$_->{name}.' '.$_->{oratype}} @cols).") ";
}
$ddl .= " distribute on random" if $args{type} =~ m/nz/i;
$ddl .= " distributed randomly" if $args{type} =~ m/gp/i;

print "$ddl\n";

