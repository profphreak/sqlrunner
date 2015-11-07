#!/usr/bin/perl

use strict;

# Copyright (c) 2015, Alex Sverdlov

# This work is free software; you can redistribute it and/or modify it
# under the terms of the GNU General Public License as published by 
# the Free Software Foundation, version 2, or higher.

# build dependency tree for SQL

#########################################################################################
# arguments.
my %args;
map {my ($n,$v)=split/=/,$_,2; $args{$n}= defined($v) ? $v:1 } @ARGV;

die qq(usage: cat *sql | $0 [cmd='command \$_'] [threads=16]) if $args{help};

# read input
{ local $/=undef; $_=<STDIN>; }

# apply environment variables.
s/\$\{(\w+)\}/$ENV{$1}/sgie;
s/\&\&?(\w+)\.?/$ENV{$1}/sgie;

# save all strings
my @str; 
s/('.*?')/push @str,$1; '#######'.$#str.'#######'/sgie;
s/(--.*?\n)/push @str,$1; '#######'.$#str.'#######'/sgie;

# cut input into statements
my @stmts = split/;/;       # cut into statements

my @stmtdep = map { [] } @stmts;
my %tbls;
for(my $i=0;$i<=$#stmts;$i++){
    local $_ = $stmts[$i];
    for my $t (keys %tbls){
        push @{$stmtdep[$i]},$tbls{$t} if m/\b(\Q$t\E)\b/;
    }
    if(m/drop\s+table\s+(\w+)/){
        $tbls{$1} = $i;
        #delete $tbls{$1};
        #$stmts[$i]="";
    }elsif(m/create\s+table\s+(\w+)/){
        #$stmts[$i]="drop table $1;\n$stmts[$i]";
        $tbls{$1} = $i;
    }elsif(m/insert\s+(into|overwrite)?\s+(table\s*)?(\w+)/){
        $tbls{$3} = $i;
    }elsif(m/analyze\s+(\w+)/){
        $tbls{$1} = $i;
    }elsif(m/alter\s+table\s+.*?rename\s+to\s+(\w+)/){
        #$stmts[$i]="drop table $1;\n$stmts[$i]";
        $tbls{$1} = $i;
    }
}

open my $out,">run.txt" or die $!;

for(my $i=0;$i<=$#stmts;$i++){

    map { s/#######(\d+)#######/$str[$1]/sgie;$_ }

    next unless $stmts[$i];
    open my $out2,">$i.sql" or die $!;
    print $out2 "-- query $i, depends on: ".join(", ",@{$stmtdep[$i]})."\n";
    print $out2 $stmts[$i]."\n;\n";
    close $out2;

    print $out "$i.sql{id=$i}{dep=".join(", ",@{$stmtdep[$i]})."}\n";
}

close $out;


