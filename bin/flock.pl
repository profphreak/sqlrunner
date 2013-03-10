#!/usr/bin/perl

# similar to util-linux flock utility
# (which some systems don't appear to have installed).

# flock.pl -nb lockfile command 

use strict;
use warnings;
use Fcntl ':flock';
use POSIX qw(strftime);

my $atim = strftime "%Y%m%d%H%M%S", localtime;  # arrival time.
my $stim = time();      # elapsed time.

my $tim;
my $nb = 0;
my ($lockfile) = ($0 =~ m/([^\/\\]+)$/);
#####################################################################
## params
while($ARGV[0] =~ m/^-/){
    $_ = shift @ARGV;
    $nb = 1 if $_ eq '-nb';     # no block.
}

$lockfile = shift @ARGV;

my $cmd = join (' ',@ARGV);

#####################################################################
# are we already running??? (exit if yes)
open my $lock,">>$lockfile" or die "$lockfile: $!";
if(!flock($lock,LOCK_EX | ($nb ? LOCK_NB : 0) )){
    close $lock;
    exit -1;
}
seek($lock, 0, 2);
$tim = strftime "%Y%m%d%H%M%S", localtime;
print $lock "$tim:S:$cmd:$atim\n";

my $rtim = time();      # run time.
`$cmd`;

$tim = strftime "%Y%m%d%H%M%S", localtime;
print $lock "$tim:E:$cmd:$?:".(time() - $stim).":".(time() - $rtim)."\n";

flock($lock,LOCK_UN);
close $lock;

