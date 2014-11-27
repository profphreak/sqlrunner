#!/usr/bin/perl

use locale; 

# run through delimited data; try to figure out data types for columns.
# output one line; fhead format.

# Alex Sverdlov
# alex@theparticle.com

# not the fastest script for this kind of stuff
# don't expect miracles if you pipe gigabytes through this script.

use strict;
use warnings;

my (%args);
map {my ($n,$v)=split/=/,$_,2; $args{$n}= defined($v) ? $v:1; } @ARGV;

if($args{help} || $args{'-help'} || $args{'--help'}){
    print "read data from stdin, and load it into db\n";
    print qq|
    Commands include:
        help: (this screen)
        delim="," 
            specify delimiter to use. default is ','
        head=field1,field2,field3, etc. column names.
        header: read first line of input as column names.
            If head or header are not specified, field names 
            will be named F1..FN
        p2  round varchar sizes to next power of 2.
        p10 round varchar sizes to next power of 10.
        v10 round varchar sizes to next multiple of 10.
    |;
    exit;
}

my $delim = $args{delim};
$delim = "," unless defined $delim;
$delim = "\t" if $delim eq '\t';

my @head;
if(defined $args{head}){
    @head = map { s/^\s+|\s+$//sgi;$_ } split/$delim/,$args{head};
}elsif(defined $args{header}){      # tells us to read header from file.
    @head = map { s/^\s+|\s+$//sgi;$_ } split/$delim/,<STDIN>;
}else{
    @head = map { "F".$_ } 1..1024;     # head is F1..F1024
}

my $cnt = 0;
my @vals;
my @ints = map { 0 } 1..1024;
my @decimals = map { 0 } 1..1024;
my @pres = map { 0 } 1..1024;   # precision
my @scales = map { 0 } 1..1024;  # max scale
my @maxlen = map { 0 } 1..1024;
my @tims = map { 0 } 1..1024;       # looks like time
my @dates = map { 0 } 1..1024;      # looks like date

while(<STDIN>){
    chomp;
    my @rec = split/\Q$delim\E/;
    $cnt++;
    for(my $i=0;$i<=$#rec;$i++){
        local $_ = $rec[$i];
        if(length($_) > 0){
            $vals[$i]++;        # not null
            if(m/^-?(\d*)(\.(\d+))?$/){
                my $len3 = defined($3) ? length($3) : 0;
                my $len1 = defined($1) ? length($1) : 0;
                $maxlen[$i] = length($_) if $maxlen[$i] < length($_);
                $scales[$i] = $len3 if $scales[$i] < $len3;
                $pres[$i] = $len1 if $pres[$i] < $len1;
                $len3 > 0 ? $decimals[$i]++ : $ints[$i]++;
                $dates[$i]++ if m/^(19|20)\d\d[01]\d[0123]\d$/;
            }elsif(m/^\d\d:\d\d:\d\d(\.\d+)?/){
                $tims[$i]++ if m/^\d\d:\d\d:\d\d(\.\d+)?/;
                $maxlen[$i] = length($_) if $maxlen[$i] < length($_);
            }elsif(m/^\d\d\d\d-\d\d-\d\d$/ || m/^\d\d-\d\d-\d\d\d\d$/){
                
                $dates[$i]++ if m/^\d\d\d\d-\d\d-\d\d$/ || 
                    m/^\d\d-\d\d-\d\d\d\d$/;
                
                $maxlen[$i] = length($_) if $maxlen[$i] < length($_);
            }else{
                $maxlen[$i] = length($_) if $maxlen[$i] < length($_);
            }
        }
    }
}

print join(',',map {
    my $t = "VARCHAR";
    my $s = $maxlen[$_];
    my $p = $pres[$_] + $scales[$_];
    my $sc = $scales[$_];
    my $v = $vals[$_];
    if(!defined($v)){
        $v = 0;
        $s = 255;
    }
    if($v == $dates[$_] && $dates[$_]>0){
        $t = 'DATE';
    }elsif($v == $ints[$_] && $ints[$_]>0){
        $t = "INT";
    }elsif($v == $decimals[$_]+$ints[$_] && ($decimals[$_]+$ints[$_])>0 ){
        $t = "NUMBER";
    }elsif($v == $tims[$_] && $tims[$_]>0){
        $t = "TIME";
    }elsif($head[$_] =~ m/price/i && $v == 0){
        $t = "NUMBER"; $s=32; $p=32; $sc=8;
    }
    $t='BIGINT' if $t eq 'INT' && $s>9; 
    $t='SMALLINT' if $t eq 'INT' && $s<5; 
    $t='TINYINT' if $t eq 'INT' && $s<3; 
    if(defined($args{p2}) && $t eq 'VARCHAR'){
        # adjust varchar lengths to next power of 2.
        $s=int(2**(int(log($s)/log(2.0))+1));
    }
    if(defined($args{p10}) && $t eq 'VARCHAR'){
        # adjust varchar lengths to next power of 2.
        $s=int(10**(int(log($s)/log(10.0))+1));
    }
    if(defined($args{v10}) && $t eq 'VARCHAR'){
        # adjust varchar lengths to next power of 2.
        $s=10*(int($s/10)+1);
    }
    
    #print "$head[$_]; t=$t s=$s p=$p sc=$sc v=$v cnt=$cnt vals=$vals[$_] dates=$dates[$_] ints=$ints[$_] decimals=$decimals[$_] tims=$tims[$_]\n";

    $head[$_]."~$t~$s~$p~$sc~".($v==$cnt ? 0:1)."~$t" 
} 0..$#vals)."\n";

