#!/usr/bin/perl

use strict;

# Copyright (c) 2005, Alex Sverdlov

# This work is free software; you can redistribute it and/or modify it
# under the terms of the GNU General Public License as published by 
# the Free Software Foundation, version 2, or higher.


# program to run commands in multiple processes
# cat commands.txt | pdist.pl cmd='somecommand $_' threads=5
# will run somecommand with argument from commands.txt (one by one) 
# with maximum 5 instances at any given time.

# can also be used in pipe mode, eg:
# cat commands.txt | pdist.pl cmd='cmd1 $_' threads=5 pipe | pdist.pl cmd='cmd2 $_' threads=2

#########################################################################################
# arguments.
my %args;
map {my ($n,$v)=split/=/,$_,2; $args{$n}= defined($v) ? $v:1 } @ARGV;

die qq{usage: [ls] | $0 [cmd='command \$_'] [threads=16]\n} unless $args{cmd};

my $ret = 0;
my $numProcs=0;
while(<STDIN>){
    chomp;
    my $maxThisTime = getMaxProcs();

    while($numProcs >= $maxThisTime){
        if(wait() > 0){
            $ret |= ($?>>8);
            $numProcs--;
        }
    }
    sleep $args{delay} if defined $args{delay};
    if(fork()){
        $numProcs++;
    }else{
        my $cmd = $args{cmd};
        $cmd=~s/\$_/$_/sgi;

        # add support for similar arguments as in gnu parallel
        my ($noext) = ($_ =~ m{(.+?)(\.[^/\\.]+)?$}s);  # {.}
        my ($basename) = ($_ =~ m{([^/\\]+)$}s);        # {/}
        my ($a,$dirname) = ($_ =~ m{((.*)[/\\])}s);     # {//}
        $dirname='.' unless length($dirname)>0; 
        my ($bnoext) = ($_ =~ m{([^/\\]+?)(\.[^/\\.]+)?$}s);    # {/.}
        $cmd=~s/\{\}/$_/sgi;
        $cmd=~s/\{\.\}/$noext/sgi;
        $cmd=~s/\{\/\}/$basename/sgi;
        $cmd=~s/\{\/\/\}/$dirname/sgi;
        $cmd=~s/\{\.\/\}/$bnoext/sgi;
        $cmd=~s/\{#\}/$./sgi;
        $cmd=~s/\{uuid\}/$a=`uuidgen`; $a=~s{^\s+|\s+}{}sgi;$a/sgie;
        $cmd=~s/\{time\}/$a=time(); $a=~s{^\s+|\s+}{}sgi;$a/sgie;

        #print "running [$cmd]\n";
        # change to chdir before running (if specified).
        chdir "$args{chdir}" if defined $args{chdir};
        `$cmd`;
        $ret |= ($?>>8);
        print "$_\n" if defined $args{pipe};
        exit $ret;
    }
}
while(wait() > 0){
    $ret |= ($?>>8);
}
exit $ret;

sub getMaxProcs {
    if(-f $args{threads}){
        open my $in,$args{threads} or return 1;
        local $_ = <$in>;
        close $in;
        chomp $_;
        return $_ if $_ >= 1 && $_ <= 4096;   # sane[?] value.
    }elsif($args{threads} =~ m/\d+/ && $args{threads} <= 4096 &&  $args{threads} >= 1){
        return int($args{threads});
    }
    return 1;
}

