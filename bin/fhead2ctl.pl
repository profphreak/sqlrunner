#!/usr/bin/perl

# table ctl grabber

# Mon Mar 17 05:19:14 EDT 2008

# Copyright (c) 2005, Alex Sverdlov

# This work is free software; you can redistribute it and/or modify it
# under the terms of the GNU General Public License as published by 
# the Free Software Foundation, version 2, or higher.




use strict;
use warnings;

my (%args);
map {my ($n,$v)=split/=/,$_,2; $args{$n}= defined($v) ? $v:1; } @ARGV;

if($args{help} || $args{'-help'} || $args{'--help'}){
    print "generate ctl file from fhead data\n";
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
        @{$r}{qw(nztype oratype oractltype)} = qw(VARCHAR VARCHAR2 CHAR);
    }elsif($r->{type} =~ m/varchar/i){
        @{$r}{qw(nztype oratype oractltype)} = 
            ("VARCHAR($r->{size})","VARCHAR2($r->{size})",$r->{size} > 100 ? "CHAR($r->{size})" : "CHAR");
    }elsif($r->{type} =~ m/char/i && $r->{size} == 0){
        @{$r}{qw(nztype oratype oractltype)} = qw(CHAR CHAR CHAR);
    }elsif($r->{type} =~ m/char/i){
        @{$r}{qw(nztype oratype oractltype)} = 
            ("CHAR($r->{size})","CHAR($r->{size})",$r->{size} > 100 ? "CHAR($r->{size})" : "CHAR");
    }elsif($r->{type} =~ m/number/i && $r->{pres} > 0 && $r->{scale} > 0){
        @{$r}{qw(nztype oratype oractltype)} = 
            ("NUMERIC($r->{pres},$r->{scale})",
            "NUMBER($r->{pres},$r->{scale})","DECIMAL EXTERNAL");
    }elsif($r->{type} =~ m/number/i && $r->{pres} > 0){
        @{$r}{qw(nztype oratype oractltype)} = 
            ("NUMERIC($r->{pres})",
            "NUMBER($r->{pres})","INTEGER EXTERNAL");
    }elsif($r->{type} =~ m/number/i){        
        @{$r}{qw(nztype oratype oractltype)} = 
            ("DOUBLE PRECISION",
            "NUMBER","DECIMAL EXTERNAL");
    }elsif($r->{type} =~ m/date/i){
        @{$r}{qw(nztype oratype oractltype)} = 
            ("DATE","DATE","DATE 'YYYYMMDD'");
    }elsif($r->{type} =~ m/timestamp/i){
        my $name = $r->{name};
        @{$r}{qw(nztype oratype oractltype)} = 
            ("TIMESTAMP","TIMESTAMP",qq{TIMESTAMP '$args{timestampformat}'}); 
    }elsif($r->{type} =~ m/time/i){
        my $name = $r->{name};
        @{$r}{qw(nztype oratype oractltype)} = 
            ("TIME","DATE",qq{DATE '$args{timeformat}'});
    }else{
        @{$r}{qw(nztype oratype oractltype)} = 
            ($r->{type},$r->{type},"CHAR");
    }
    $r
} split /,/,$input;

# example
# LOAD DATA
#  INFILE *
#  INTO TABLE modified_data
#  (  rec_no                      "my_db_sequence.nextval",
#     region                      CONSTANT '31',
#     time_loaded                 "to_char(SYSDATE, 'HH24:MI')",
#     data1        POSITION(1:5)  ":data1/100",
#     data2        POSITION(6:15) "upper(:data2)",
#     data3        POSITION(16:22)"to_date(:data3, 'YYMMDD')"
#  )
# "to_date(:col2, 'YYYY-MM-DD HH24:MI:SS')"


$args{delim}="," unless defined $args{delim};

my $ctl = "LOAD DATA\nINFILE '$args{file}'\n".
 ($args{noappend} ? "" : "APPEND\n").
 "INTO TABLE $args{table}\n".
 ($args{partition} ? "PARTITION ($args{partition})\n" : "").
 ($args{trunc} ? "TRUNCATE\n" : "").
qq{FIELDS TERMINATED BY "$args{delim}" OPTIONALLY ENCLOSED BY '"' trailing nullcols\n(}.
    join(",\n",map { "    ".$_->{name}.' '.$_->{oractltype} } @cols)."\n)\n";
print "$ctl\n";


if($args{par}){
    my @f=split/\./,$args{file};
    pop @f;
    my $f = join(".",@f);
    $f = $args{par} unless $f;

    $args{ctlfile}="$f.ctl" unless defined $args{ctlfile};
    $args{logfile}="$f.log" unless defined $args{logfile};
    $args{badfile}="$f.bad" unless defined $args{badfile};
    $args{userid}="" unless $args{userid};

    open my $out,">$args{par}.par" or die $!;
    print $out "userid = '$args{userid}'\n";
    print $out "control = '$args{ctlfile}'\n";
    print $out "log= '$args{logfile}'\n";
    print $out "bad= '$args{badfile}'\n";
    print $out "direct = true\n" if $args{direct};  # allow direct loads
    print $out "silent = (discards, feedback)\n";
    print $out "errors = 50\n";
    close $out;
}

