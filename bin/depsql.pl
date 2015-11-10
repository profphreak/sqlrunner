#!/usr/bin/perl

use strict;

# Copyright (c) 2015, Alex Sverdlov

# This work is free software; you can redistribute it and/or modify it
# under the terms of the GNU General Public License as published by 
# the Free Software Foundation, version 2, or higher.

# build dependency tree for SQL
# output individual sql files, and a script to pdist.

#########################################################################################

# arguments.
my %args;
map {my ($n,$v)=split/=/,$_,2; $args{$n}= defined($v) ? $v:1 } @ARGV;

$args{prefix} = "" unless defined $args{prefix};

die qq(usage: cat *sql | $0 
    options: 
        gensql      if specified, generates a .sql file for each query.
        pdistfile   if specified, creates a file suitable to pipe into pdist.pl.
    
        debug       dump output to stdout.
        prefix      output files have prefix: default is empty.        
) if $args{help};

# read input
{ local $/=undef; $_=<STDIN>; }

# apply environment variables.
s/\$\{(\w+)\}/$ENV{$1}/sgie;
s/\&\&?(\w+)/$ENV{$1}/sgie;

# save all strings
my @str; 
s/(--.*?)\n/push @str,$1; '#######'.$#str."#######\n"/sgie;
s/('.*?')/push @str,$1; '#######'.$#str.'#######'/sgie;

# cut input into statements
my @stmts = split/;/;       # cut into statements

my @stmtdep = map { [] } @stmts;
my (%usedtbls, %tbls);
for(my $i=0;$i<=$#stmts;$i++){
    local $_ = $stmts[$i];
    for my $t (keys %tbls){
        if(m/\b(\Q$t\E)\b/i){
            push @{$stmtdep[$i]},$tbls{$t};
            $usedtbls{$t} = [] unless $usedtbls{$t};
            push @{$usedtbls{$t}},$i;
        }
    }
    if(m/drop\s+table\s+(\w+)/i){
        my $t = lc $1;
        push @{$stmtdep[$i]},grep { $_ != $i } @{$usedtbls{$t}} if $usedtbls{$t};
        delete $usedtbls{$t};
        $usedtbls{$t} = undef;
        $tbls{$t} = $i;
    }elsif(m/create\s+table\s+(\w+)/i){
        $tbls{lc $1} = $i;
    }elsif(m/insert\s+(into|overwrite)\s+(table\s*)?(\w+)/i){
        $tbls{lc $3} = $i;
    }elsif(m/analyze(\s*table)\s+(\w+)/i){
        $tbls{lc $2} = $i;
    }elsif(m/alter\s+table\s+(\w+).*?rename\s+to\s+(\w+)/i){
        my $t = lc $1;
        push @{$stmtdep[$i]},grep { $_ != $i } @{$usedtbls{$t}} if $usedtbls{$t};
        delete $usedtbls{$t};
        $usedtbls{$t} = undef;
        $tbls{lc $2} = $i;
    }
}

my $out;
if($args{pdistfile}){
    open $out,">$args{pdistfile}" or die $!;
}

for(my $i=0;$i<=$#stmts;$i++){
    local $_ = $stmts[$i];
    s/^\s+|\s+$//sgi; 
    if($_){
        my %dups;
        my @depdedup = grep { ! $dups{$_}++ } @{$stmtdep[$i]};
        $_ = $stmts[$i];
        s/#######(\d+)#######/$str[$1]/sgie; 
        if($args{debug}){
            print "-- query $i, depends on: ".join(", ",@depdedup)."\n";
            print $_."\n;\n";
        }
        if($args{gensql}){
            open my $out2,">$args{prefix}$i.sql" or die $!;    
            print $out2 "-- query $i, depends on: ".join(", ",@depdedup)."\n";
            print $out2 $_."\n;\n";
            close $out2;
        }
        print $out "$args{prefix}$i.sql{id=$i}{dep=".join(",",@depdedup)."}\n" if $out;
    }
}
close $out if $out;

