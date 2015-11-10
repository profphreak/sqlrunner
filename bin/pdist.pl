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

die qq(usage: [ls] | $0 [cmd='command \$_'] [threads=16]

    in command, 
        input line replacements: \$_, {}
        {.}     strips file extention.
        {/}     strips directory; basename.
        {//}    strips filename, leaving directory; dirname
        {./}    strips directory and file extention.
        {#}     replaced with line number.
        {uuid}  replaced with uuid; uuidgen command must be installed.
        {time}  replaced with current time; seconds since 1970.
        {tim}   replaced with current time; YYYYMMDDHHMISS format.

        {dep=3,4,5} dependency definition; this command depends on those command ids.
        {id=whatever} define current command id, default is line number.

    threads parameter may be a filename. on each iteration $0 will check for changes.
    
    dieonfail   if specified, script dies if any children terminate abnormally. 
    stdout      if specified, will display debug log to stdout.
    delay=N     if specified, waits N seconds between jobs.

    log=FILE    if specified, writes event log (can be used for recovery).
    recover     if specified, attempts to recover using the log file.
) unless $args{cmd};

my $ret = 0;
my $numProcs=0;
my (@cmdqueue,%state,%pid2id);

# attempt to recover from a previously created log file.
if($args{recover} && -f $args{log}){
    open my $in,$args{log} or die "unable to open log file: $args{log}\n";
    while(<$in>){
        chomp;
        $state{$1} = {done=>1} if m/DONE.*\sid=([^\s,]+)/sgi;
    }
    close $in;
}


while(1){

    my $maxThisTime = getMaxProcs();        # get current limit
    while($numProcs >= $maxThisTime){       # while over limit
        waitForChild();
        $maxThisTime = getMaxProcs();       # update limit (perhaps changed). 
    }

    # we have free slot to run something.

    # if failed depencendy, then fail job
    for my $o (@cmdqueue){
        map {  $o->{failed} = 1 if $state{$_}{failed} } @{$o->{dep}};        
    }
    
    # removed failed jobs from command queue.
    @cmdqueue = grep { !$_->{failed} } @cmdqueue;

    # pick a command from command queue that we can run right now (all dependencies satisfied)
    my ($cmdo) = grep { my @a=grep { !$state{$_}{done} } @{$_->{dep}};$#a==-1 } @cmdqueue;

    # if none exists, while cannot find command, read stdin, keep on adding to command queue.
    while(!$cmdo){
        $_ = <STDIN>;   chomp;      # read input.
        last unless defined $_;
        
        my $o = { id=>$. };
        $o->{id} = $1 if s/\{id=\s*(.*?)\s*\}//si;
        $o = $state{ $o->{id} } if defined $state{ $o->{id} };
        $state{ $o->{id} } = $o;
        $o->{dep} = [split/\s*,\s*/,$1] if s/\{dep=\s*(.*?)\s*\}//si;
        $o->{cmd} = $_;
        $o->{tim} = gettim();
        if($o->{done}){ # is it already done?
            writelog("$o->{tim}: RECOVERSKIPPING: ".
                join(", ",map { $_."=".$o->{$_} } qw(id pid tim cmd))."\n");
        }else{
            push @cmdqueue,$o;
        }

        # pick a command from command queue that we can run right now (all dependencies satisfied)
        ($cmdo) = grep { my @a=grep { !$state{$_}{done} } @{$_->{dep}};$#a==-1 } @cmdqueue;
    }

    # if still nothing to run...
    if(!$cmdo){
        last unless @cmdqueue;      # is there stuff to do?
        last unless $numProcs > 0;  # are we running anything?
        waitForChild();             # wait for child to finish (perhaps dependency?)
    }

    if($cmdo){                      # have a command to run

        if($args{stdout}){        
            my $tim = gettim();
            print "$tim:Starting:$numProcs, running id:$cmdo->{id}, cmd:$cmdo->{cmd}, dep=[".join(",",@{$cmdo->{dep}})."]\n";
            for my $id (grep { $state{$_}{running} } sort keys %state){
                print "$tim:\tRunning:$state{$id}{id}, pid:$state{$id}{pid}, cmd:$state{$id}{cmd}, dep=[".join(",",@{$state{$id}{dep}})."]\n";
            }
            print "\n";
        }

        # remove it from command queue
        @cmdqueue = grep { $_->{id} ne $cmdo->{id} } @cmdqueue;

        sleep $args{delay} if defined $args{delay};
        my $pid = fork();
        if($pid){           # parent
            $cmdo->{pid}=$pid;
            $pid2id{$pid}=$cmdo->{id};
            $state{$cmdo->{id}}{running} = 1;
            $state{$cmdo->{id}}{tim} = gettim();
            $numProcs++;
            writelog("$cmdo->{tim}: STARTED: ".
                join(", ",map { $_."=".$cmdo->{$_} } 
                    qw(id pid tim cmd))."\n");
        }else{              # child
            local $_ = $cmdo->{cmd};
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
            $cmd=~s/\{tim\}/$a=gettim();$a/sgie;

            # change to chdir before running (if specified).
            chdir "$args{chdir}" if defined $args{chdir};
            `$cmd`;
            $ret |= ($?>>8);
            exit $ret;
        }
    }
}

while(waitForChild() > 0){}

exit $ret;

# wait for child; return childid.
sub waitForChild {
    my $pid = wait();                   # wait for child to terminate    
    if($pid > 0){                       # if got child process id
        my $err = $?>>8;
        my $id = $pid2id{$pid};         # get pid to command id mapping.
        delete $pid2id{$pid};   # child no longer exists            

        my $chld=$state{$id};
        $chld->{etim} = gettim();       # record when process finished.
        $chld->{ret} = $err;            # record return code.
        $chld->{running} = 0;         # not running anymore.
        if( $err ){        # command failed
            $chld->{failed} = 1;         # record failure.
            writelog("$chld->{etim}: FAILED: ".join(", ",map { $_."=".$chld->{$_} } qw(id pid ret tim etim cmd))."\n");
        }else{          # command succeeded. 
            $chld->{done} = 1;
            writelog("$chld->{etim}: DONE: ".
                join(", ",map { $_."=".$chld->{$_} } 
                    qw(id pid ret tim etim cmd))."\n");
            print "$chld->{cmd}\n" if defined $args{pipe};
        }
        $ret |= $err;                # keep track of global return code.
        $numProcs--;                    # free up slot for next job.
        die "child failed: childid:$id: pid:$pid line:$chld->{cmd}\n" if $args{dieonfail} && $ret;     # DIE!!!
    }
    return $pid;
}

sub getMaxProcs {
    if(-f $args{threads}){          # if threads is a file... read that.
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

# grab current timestamp
sub gettim {
    my ($sec,$min,$hour,$mday,$mon,$year,$wday,$yday,$isdst) = localtime();
    return sprintf("%04d%02d%02d%02d%02d%02d",$year+1900,$mon+1,$mday,$hour,$min,$sec);
}

sub writelog {
    my ($s) = @_;
    open my $out,">>$args{log}";
    print $out $s;
    close $out;
}


