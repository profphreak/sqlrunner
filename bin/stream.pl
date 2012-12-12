#!/usr/bin/perl

# EXPERIMENTAL CODE; FULL OF BUGS

# calculate multiple windowing functions in streaming single pass
# Part of SQLRunner

# Copyright (c) 2012, Alex Sverdlov

# Wed Dec 12 02:18:01 EST 2012

# This work is free software; you can redistribute it and/or modify it
# under the terms of the GNU General Public License as published by 
# the Free Software Foundation, version 2, or higher.


use strict;
use warnings;

for(my $i=0;$i<$#ARGV;$i++){
    if($ARGV[$i] =~ m/=$/ && defined($ARGV[$i+1]) && $ARGV[$i+1] !~ m/^(\w+)=/i){
        $ARGV[$i] .= $ARGV[$i+1];       # only squish if next value has no '=' sign.
        $ARGV[$i+1] = undef;
    }
}

my (%args);
map {my ($n,$v)=split/=/,$_,2; $args{$n}=defined($v) ? $v:1; } grep { defined($_) } @ARGV;


if($args{help} || $args{'-help'} || $args{'--help'}){
    print "stream script\ncalculate multiple windowing functions in streaming single pass\n";
    print qq|
    Commands include:
        delim= specify a deliminter, default is ','
        pby= specify partition columns, e.g. pby=1,2, default none.
        oby= specify order by column, e.g. oby=7, default none.
        hive; specify hive IO, e.g. \\t as delim, and \\N as null.
        nullval= specify constant to use for NULL value (default empty).
        append= specify what column to append to output,
            e.g. "sum(4, 20 pre), stddev(4, 20 pre)"        
        help: (this screen)

        e.g.:
        cat some.csv \| stream.pl pby=1,2 append="avg(4,20 pre),stddev(4,20 pre)"
        --the above says "partition by columns 1 and 2, and 
        --append a 20 row moving average and standard deviation to output.

        Function format: name(input_column, params),
        
        Supported params:
            N pre, e.g. 20 pre, 20 preceding
            N fol, e.g. 20 fol, 0 fol, 0 following
                negative values imply "unbounded", e.g. -1 pre.
            ignore nulls, inulls
            range, rows
            
        Supported functions: 
            row_number  - assign distinct number to every row in partition
            rank        - assign a rank; ranks() for string version
                        - specify "oby" (order by) column.
            dense_rank  - assign a dense rank; dense_ranks() for string version
            count       - count all records within partition
                        - (expensive, reads entire partition into ram).
            last_value  - gets last value in window (ignore nulls supported).                        
            lead        - grab next record.
            lag         - grab last record.
            first_value - first value in window
            avg         - average of non-null numeric values
            stddev      - sample standard deviation
            sum         - cummulative sum
            min/max     - self explanatory :-)

        Hadoop Hive usage:
            append 20 record moving average and sd
            similar to:
            avg(col4) over (partition by col1,col2 order by col3 
                rows between 20 preceding and current row) avg20,
            stddev(col4) over (partition by col1,col2 order by col3 
                rows between 20 preceding and current row) sd20,

        add file $0;

        drop table tmp_table;
        create table tmp_table as
            select
                transform(col1,col2,col3,col4)
                using 'stream.pl pby=1,2 oby=3 hive append="avg(4,20 pre),stddev(4,20 pre)"'
                as (col1 string,col2 string,col3 string,col4 string,
                    avg20 double,sd20 double)
            from (
                select col1,col2,col3,col4
                from some_table
                distribute by col1,col2
                sort by col1,col2,col3,col4
            ) a;
\n|;
    exit;
}


# define some globals
my $delim = defined($args{delim}) ? $args{delim} : (defined($args{hive}) ? "\t" : ',' );
my $pby = defined($args{pby}) ? [ split/,/,$args{pby} ] : [];
my $oby = defined($args{oby}) ? $args{oby} : 0;     # default order by column
my $nullval = defined($args{nullval}) ? $args{nullval} : (defined($args{hive}) ? '\N' : '' );

$delim = "\t" if $delim eq '\t';

$args{append} = "" unless defined($args{append});   # pipe data through unless something to append.


# user defined analytical functions :-)
my $udaf = {
    row_number => {
        init=> sub { my ($o) = @_; $o->{cnt}=0; },
        push=> sub { my ($o,$v) = @_; $o->{cnt}++; },
        set=> sub { my ($o,$r) = @_; $$r = $o->{cnt} },
    },
    dense_rank => {
        init=> sub { my ($o) = @_; $o->{cnt}=1; $o->{incol}=defined($oby) ? $oby : 0; },
        push=> sub { my ($o,$v) = @_; 
            $o->{l}=$v unless defined($o->{l}); 
            $o->{cnt}++ if $v > $o->{l}; $o->{l}=$v;
        },
        set=> sub { my ($o,$r) = @_; $$r = $o->{cnt} },
    },
    rank => {
        init=> sub { my ($o) = @_; $o->{cnt}=1; $o->{cnt2}=1; $o->{incol}=defined($oby) ? $oby : 0; },
        push=> sub { my ($o,$v) = @_; $o->{l}=$v unless defined($o->{l}); $o->{cnt}=$o->{cnt2} if $v > $o->{l}; $o->{l}=$v; $o->{cnt2}++; },
        set=> sub { my ($o,$r) = @_; $$r = $o->{cnt} },
    },
    dense_ranks => {
        init=> sub { my ($o) = @_; $o->{cnt}=1; $o->{incol}=defined($oby) ? $oby : 0; },
        push=> sub { my ($o,$v) = @_; 
            $o->{l}=$v unless defined($o->{l}); 
            $o->{cnt}++ if $v gt $o->{l}; $o->{l}=$v;
        },
        set=> sub { my ($o,$r) = @_; $$r = $o->{cnt} },
    },
    ranks => {
        init=> sub { my ($o) = @_; $o->{cnt}=1; $o->{cnt2}=1; $o->{incol}=defined($oby) ? $oby : 0; },
        push=> sub { my ($o,$v) = @_; $o->{l}=$v unless defined($o->{l}); $o->{cnt}=$o->{cnt2} if $v gt $o->{l}; $o->{l}=$v; $o->{cnt2}++; },
        set=> sub { my ($o,$r) = @_; $$r = $o->{cnt} },
    },
    count => {
        init=> sub { my ($o) = @_; $o->{cnt}=0; $o->{preceding}=-1; $o->{following}=-1; },
        push=> sub { my ($o,$v) = @_; $o->{cnt}++; },
        set=> sub { my ($o,$r) = @_; $$r = $o->{cnt} },        
    },
    last_value => {
        init=> sub { my ($o) = @_; $o->{a}=[]; },
        push=> sub { my ($o,$v) = @_; 
            if($o->{inull} && !defined($v) && $#{$o->{a}}>=0) {     # ignore nulls
                push @{$o->{a}},$o->{a}[-1] ; 
            } else { 
                push @{$o->{a}},$v; 
            } 
        },
        shift=> sub { my ($o,$v) = @_; shift @{$o->{a}}; },
        set=> sub { my ($o,$r) = @_; $$r = $o->{a}[-1] if $#{$o->{a}}>=0 },
    },    
    last_value_u => {
        init=> sub { my ($o) = @_; },
        push=> sub { my ($o,$v) = @_; $o->{v} = $v unless $o->{inull} && !defined($v); },
        set=> sub { my ($o,$r) = @_; $$r = $o->{v} },
    },
    lead => { # lead is last_value 0 pre 1 fol
        init=> sub { my ($o) = @_; $o->{a}=[]; $o->{preceding}=0; $o->{following}=1; },
        push=> sub { my ($o,$v) = @_; push @{$o->{a}},$v; },
        shift=> sub { my ($o,$v) = @_; shift @{$o->{a}}; },
        set=> sub { my ($o,$r) = @_; $$r = $o->{a}[-1] if $#{$o->{a}}>=1 },
    },
    lag => { # lag is first_value 1 pre 0 fol
        init=> sub { my ($o) = @_; $o->{a}=[]; $o->{preceding}=1; $o->{following}=0; },
        push=> sub { my ($o,$v) = @_; push @{$o->{a}},$v; },
        shift=> sub { my ($o,$v) = @_; shift @{$o->{a}}; },
        set=> sub { my ($o,$r) = @_; $$r = $o->{a}[0] if $#{$o->{a}}>=1 },
    },    
    # todo: rank
    first_value => { 
        init=> sub { my ($o) = @_; $o->{a}=[]; },
        push=> sub { my ($o,$v) = @_; push @{$o->{a}},$v; },
        shift=> sub { my ($o,$v) = @_; shift @{$o->{a}}; },
        set=> sub { my ($o,$r) = @_; $$r = $o->{a}[0] if $#{$o->{a}}>=0 },
    },
    first_value_u => {
        init=> sub { my ($o) = @_; },
        push=> sub { my ($o,$v) = @_; $o->{v} = $v unless defined $o->{v}; },
        set=> sub { my ($o,$r) = @_; $$r = $o->{v} },
    },
    avg => {
        init=> sub { my ($o) = @_; $o->{sum}=0; $o->{cnt}=0; },
        push=> sub { my ($o,$v) = @_; if(defined($v)){ $o->{sum}+=$v; $o->{cnt}++; } },
        shift=> sub { my ($o,$v) = @_; if(defined($v)){ $o->{sum}-=$v; $o->{cnt}--; } },
        set=> sub { my ($o,$r) = @_; 
            my $v = $o->{sum} / $o->{cnt} if $o->{cnt}!=0;
            if(defined($v)){
                $$r = abs($v) < 0.000001 ? 0 : sprintf("%-.8f",$v);
            }
        },
    },
    stddev => {
        init=> sub { my ($o) = @_; $o->{sum}=0; $o->{sum2}=0; $o->{cnt}=0; },
        push=> sub { my ($o,$v) = @_; if(defined($v)) { $o->{sum}+=$v; $o->{sum2}+=$v*$v; $o->{cnt}++; } },
        shift=> sub { my ($o,$v) = @_; if(defined($v)) { $o->{sum}-=$v; $o->{sum2}-=$v*$v; $o->{cnt}--; } },
        set=> sub { my ($o,$r) = @_; 
            my $v=($o->{sum2} - $o->{sum}*$o->{sum}/$o->{cnt})/($o->{cnt}-1) if $o->{cnt}>=2;
            if(defined($v)){
                $$r = abs($v) < 0.000001 ? 0 : sprintf("%-.8f",sqrt($v));
            }
        },
    },
    sum => {
        init=> sub { my ($o) = @_; $o->{sum}=0;  },
        push=> sub { my ($o,$v) = @_; if(defined($v)) { $o->{sum}+=$v; } },
        shift=> sub { my ($o,$v) = @_; if(defined($v)) { $o->{sum}-=$v; } },
        set=> sub { my ($o,$r) = @_; my $v = $o->{sum}; 
        $$r = $v==int($v) ? sprintf("%d",$v) : sprintf("%-.8f",$v) },
    },
    max_u => {
        push=> sub { my ($o,$v) = @_; $o->{max}=$v if !defined($o->{max}) || $o->{max}<$v },
        set=> sub { my ($o,$r) = @_; $$r = $o->{max}; },
    },
    min_u => {
        push=> sub { my ($o,$v) = @_; $o->{min}=$v if !defined($o->{min}) || $o->{min}>$v },
        set=> sub { my ($o,$r) = @_; $$r = $o->{min}; },
    },
    max => {
        init=> sub { my ($o) = @_; $o->{buf}=[]; },
        push=> sub { my ($o,$v) = @_; push @{$o->{buf}},$v; $o->{max}=$v if !defined($o->{max}) || $o->{max}<$v },
        shift=> sub { my ($o,$v) = @_; shift @{$o->{buf}};  
            if($v == $o->{max}){    # max may have changed
                my $m = $o->{buf}[0] if $#{$o->{buf}};
                map { $m = $_ if $m < $_ } @{$o->{buf}};
                $o->{max} = $m;
            }
        },
        set=> sub { my ($o,$r) = @_; $$r = $o->{max}; },
    },
    min => {
        init=> sub { my ($o) = @_; $o->{buf}=[]; },
        push=> sub { my ($o,$v) = @_; push @{$o->{buf}},$v; $o->{min}=$v if !defined($o->{min}) || $o->{min}>$v },
        shift=> sub { my ($o,$v) = @_; shift @{$o->{buf}};
            if($v == $o->{min}){    # max may have changed
                my $m = $o->{buf}[0] if $#{$o->{buf}};
                map { $m = $_ if $m > $_ } @{$o->{buf}};
                $o->{min} = $m;
            }
        },
        set=> sub { my ($o,$r) = @_; $$r = $o->{min}; },
    },
};


# parse ``append'' clause
my $funcs = [];
while($args{append} =~ m/(\w+)\s*\((.*?)\)\s*,?/sgi){
    my $conf = {
        current_row=>1, 
        following=>0,
        preceding=>-1,  # unbounded preceding
        oby=>0,         # rows
        func=>$1,
        incol=>0,
    };
    die $conf->{func}." function undefined\n" unless defined($udaf->{$conf->{func}});
    my ($incol,$attribs) = split/,/,$2,2;
    $attribs = "" unless defined $attribs;
    $conf->{incol} = $incol if defined $incol;  #default 0
    $attribs =~ s/curr(ent)?\s*(row)?/current_row/sgi;
    $conf->{current_row} = 0 if $attribs =~ m/exc(lude)?\s*current_row/si;
    $conf->{oby} = $oby if $attribs =~ m/range/si;
    $conf->{preceding} = $1 if $attribs =~ m/(\w+)\s+pre(ceding)?/si;
    $conf->{following} = $1 if $attribs =~ m/(\w+)\s+fol(lowing)?/si;
    $conf->{inull} = 1 if $attribs =~ m/inull|ignore\s*null/si;
    $conf->{id} = $#$funcs + 1;
    push @$funcs,$conf;
}

# read lines from input stream
sub mkinputit ($) {
    my ($in) = @_;
    return sub {
        return <$in>;
    }
}

# iterator map 
sub imap (&$) {
    my ($f, $it) = @_;
    return sub {
        local $_ = $it->();
        return unless defined $_;
        return $f->();
    }
}

# make partition iterator
sub mkprtreader ($) {
    my ($it) = @_;      # input iterator
    my $peek = $it->();
    our $input_columns = $#$peek;  # number of input columns
    return sub {
        my $cpby;
        return unless $peek;    # no more partitions
        return sub {
            local $_ = $peek;
            return unless defined $_; # nothing read            
            $cpby = $_->[-1] unless defined $cpby;  # define current partition
            return if defined($cpby) && $cpby ne $_->[-1];  # if moved to diff partition
            $peek = $it->();    # peek into next record
            pop @$_;            # remove partition column
            return $_;
        }
    }
}

# if reading beyond buffer, extend buffer, else read from buffer
sub mkbufreader ($$) {
    my ($it,$buf) = @_;
    my $idx;
    return sub {
        unless($#$buf>=0){  # if buffer is empty, grab a record.
            local $_ = $it->();
            return unless defined $_;
            push @$buf,$_;
        }
        $idx = $buf->[0][0] - 1 unless defined $idx;    # next will point to first
        $idx = ($idx - $buf->[0][0]) + 1;   # next record
        while($idx > $#$buf){   # while we haven't reached proper index
            local $_ = $it->();     # read record
            return unless defined $_;
            push @$buf,$_;
        }
        local $_ = $buf->[$idx];    # grab
        $idx = $_->[0];     # setup index for next read.
        return $_;
    }
}

# read v range of records at a time
sub mkrangereader {
    my ($it,$v,$push) = @_;
    my $buf = [];
    my $peek = $it->();
    return sub {
        while(defined($peek) && ( $#$buf<0 || $v<0 || abs($buf->[0][0]-$peek->[0]) <= $v )){
            local $_ = $peek;
            $push->() if defined $push;   # call push method.
            push @$buf,$peek;  # save
            $peek = $it->();    # peek next record
        }
        return shift @$buf; # grab next from buffer
    }
}

# read window of records
sub mkwinreader {
    my ($it,$conf) = @_;
    $conf = { %$conf };         # conf is state
    our $input_columns;
    my $fname = $conf->{func};
    my $func = $udaf->{$fname};
    
    # is there an unbounded preceding version of this func?
    $func = $udaf->{$fname.'_u'} if $conf->{preceding} < 0 && defined($udaf->{$fname.'_u'});
    $func->{init}($conf) if $func->{init};
    my ($oby,$incol) = @{$conf}{qw(oby incol)};
    my $outcol = $input_columns + $conf->{id};
    $it = imap(sub { [$_->[$oby], $_->[$incol], \$_->[$outcol] ] },$it);

    if($conf->{following} != 0){
        my $f = mkrangereader($it,$conf->{following}, sub { $func->{push}($conf,$_->[1]); } );
        if($conf->{preceding} < 0){
            return imap { $func->{set}($conf,$_->[2]);$_ } $f;
        }elsif($conf->{preceding} > 0){
            return mkrangereader(
                mkrangereader($f,$conf->{preceding},sub { $func->{set}($conf,$_->[2]); } ),
                0,
                sub { $func->{shift}($conf,$_->[1]); }
            );
        }elsif($conf->{preceding} == 0 && $conf->{current_row}){
            return mkrangereader( mkrangereader($f,0,sub { $func->{set}($conf,$_->[2]); }), 0,
                sub { $func->{shift}($conf,$_->[1]); } );
        }elsif($conf->{preceding} == 0 && !$conf->{current_row}){
            return mkrangereader( mkrangereader($f,0,sub { $func->{shift}($conf,$_->[1]); }), 0,
                sub { $func->{set}($conf,$_->[2]); } );        
        }
    }elsif($conf->{preceding} < 0){
        if($conf->{current_row}){
            return imap { $func->{push}($conf,$_->[1]); $func->{set}($conf,$_->[2]);$_ } $it;
        }else{
            return imap { $func->{set}($conf,$_->[2]); $func->{push}($conf,$_->[1]);$_ } $it;
        }
    }elsif($conf->{preceding} > 0){
        return mkrangereader(
            mkrangereader($it, $conf->{preceding},
                $conf->{current_row} ? 
                sub { $func->{push}($conf,$_->[1]); $func->{set}($conf,$_->[2]) } :
                sub { $func->{set}($conf,$_->[2]); $func->{push}($conf,$_->[1]) } 
            ), 
            0,  # range 0  
            sub { $func->{shift}($conf,$_->[1]); } 
        );
    }else{
        return imap { $func->{push}($conf,$_->[1]); $func->{set}($conf,$_->[2]); $func->{shift}($conf,$_->[1]);$_ } $it;
    }
}


my $prtit = mkprtreader 
    imap { push @$_,join($delim,@{$_}[@$pby]); map { $_=undef if $_ eq $nullval } @$_; $_ } 
    imap { my $a=[ $., split/$delim/ ]; chomp($a->[-1]); $a } 
    mkinputit(*STDIN);

while(my $it = $prtit->()){     # for all partition
    my $buf = [];
    my @its = map { mkwinreader(mkbufreader($it,$buf),$_) } @$funcs;
    push @its,mkbufreader($it,$buf) unless $#its >= 0;    # default iterator to pull records.
    map { $_->() } @its;
    while($#$buf >= 0){        
        local $_ = shift @$buf;
        shift @$_;
        print join($delim,map { defined($_) ? $_ : $nullval } @$_)."\n";        
        map { $_->() } @its;
    }
}

