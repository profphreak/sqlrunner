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

die qq(usage: 
        cat *sql | $0 gensql pdistfile=run.txt - 
        $0 gensql pdistfile=run.txt file1.sql file2.sql file3.sql ...

    options: 
        gensql      if specified, generates a .sql file for each query.
        pdistfile   if specified, creates a file suitable to pipe into pdist.pl.
        -           dash specifies to read stdin.
        
        debug       dump output to stdout.
        prefix      output files have prefix: default is empty.
) if $args{help};

my @str;            # all strings
my @stmts;          # all statements

# for everything on stdin, and all files on command line.
for my $p (@ARGV){
    $_ = undef;         # new input
    if(-f $p){
        open my $in,$p;    # if user gives us filename, then read that file.
        next unless $p; # skip silently 
        local $/=undef; # slurp mode
        $_=<$in>;
        close $in;      # close file.
    }elsif($p eq '-'){
        local $/=undef; 
        $_=<STDIN>;   # if user specifies - at command line, read stdin.
    }else{
        next;           # skip this arg
    }

    next unless $_;    # skip unless read stuff

    # apply environment variables.
    s/\$\{(\w+)\}/$ENV{$1}/sgie;
    s/\&\&?(\w+)/$ENV{$1}/sgie;

    s/(--.*?)\n/push @str,$1; '#######'.$#str."#######\n"/sgie;
    s/('.*?')/push @str,$1; '#######'.$#str.'#######'/sgie;

    # cut input into statements
    push @stmts,split/;/;       # cut into statements
    $stmts[-1] = undef;         # remove stuff after last statement (indicate EOF)

    # undef in statement list indicates end of file; variable get reset, etc.  
}

my @stmtdep = map { [] } @stmts;
my (%usedtbls, %tbls);
for(my $i=0;$i<=$#stmts;$i++){
    local $_ = $stmts[$i];
    next unless $_;
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
        $tbls{lc $2} = $i;
    }
}

my $out;
if($args{pdistfile}){
    open $out,">$args{pdistfile}" or die $!;
}


my @metacommands;   # add file, set variable.

for(my $i=0;$i<=$#stmts;$i++){
    local $_ = $stmts[$i];

    unless(defined($_)){    # reached end of file, reset variables.
        @metacommands = ();
        next;
    }
    s/^\s+|\s+$//sgi;
    if($_){
        my %dups;
        my @depdedup = grep { ! $dups{$_}++ } @{$stmtdep[$i]};
        $_ = $stmts[$i];
        s/^\s+|\s+$//sgi;
        s/#######(\d+)#######/$str[$1]/sgie;

        if(m/^\s*set\s+/si || m/\n\s*set\s+/si || m/^\s*add\s+file/si || m/\n\s*add\s+file/si){
            push @metacommands,$_;
            next;
        }

        if($args{debug}){
            print "-- query $i, depends on: ".join(", ",@depdedup)."\n";
            print join("",map { "$_;\n" } @metacommands);
            print $_."\n;\n";
        }
        if($args{gensql}){
            open my $out2,">$args{prefix}$i.sql" or die $!;            
            print $out2 "-- query $i, depends on: ".join(", ",@depdedup)."\n";
            print $out2 join("",map { "$_;\n" } @metacommands);
            print $out2 $_."\n;\n";
            close $out2;
        }
        print $out "$args{prefix}$i.sql{id=$i}{dep=".join(",",@depdedup)."}\n" if $out;
    }
}
close $out if $out;


