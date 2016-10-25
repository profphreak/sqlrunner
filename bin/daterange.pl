#!/usr/bin/perl

use strict;
use warnings;
use Date::Manip;
# &Date_Init("TZ=EST");

my ($start,$end) = @ARGV;

$end = $start unless defined $end;

$start = UnixDate(Date_PrevWorkDay($start,0),'%Y%m%d');
$end = UnixDate(Date_PrevWorkDay($end,0),'%Y%m%d');

my $s = $start;     # original start
($start,$end) = ($end,$start) if Date_Cmp($start,$end) < 0;

my @dates;
for (my $tdate = $start; Date_Cmp($tdate,$end) >= 0;
    $tdate = UnixDate(Date_PrevWorkDay($tdate,1),'%Y%m%d')){
    push @dates,$tdate;
}
@dates = reverse @dates if $s ne $start;

print join("\n",@dates)."\n";

