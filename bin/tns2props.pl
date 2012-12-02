#!/usr/bin/perl

# script to convert oracle TNS file into a properties with JDBC urls.
#

# Copyright (c) 2005, Alex Sverdlov

# This work is free software; you can redistribute it and/or modify it
# under the terms of the GNU General Public License as published by 
# the Free Software Foundation, version 2, or higher.


# read everything.
{ local $/=undef; $_ = <> }
s/#.*?\n/\n/sg;
my $cnt=0;
s{([\(\)])}{ $1 eq '(' ? "!(".(++$cnt)."!" : "!)".($cnt--)."!" }sgie;

while(m|([\w\.]+)\s*=\s*(\s*!\(1!.*?!\)1!\s*)|sgi){
    my ($name,$value) = ($1,$2);
    $value =~ s/!([\(\)])\d+!/$1/sgi;
    $value =~ s/\s+//sgi;
    $name =~ s/^(\w+).*/$1/;    # leave only first part of: name.blah.com to name.
    $name = lc $name;
    print $name.'_tns='.$value."\n";
    print $name.'_dbname='.$name."\n";
    print $name.'_url=jdbc:oracle:thin:@&&'.$name."_tns\n";
    print $name."_driver=oracle.jdbc.driver.OracleDriver\n\n";
}

