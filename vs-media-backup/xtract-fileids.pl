#!/usr/bin/perl
use strict;
use warnings;

foreach(<STDIN>){
    /Some\((\w{2}-\d+)\)\)/;
    print "curl -X DELETE http://pluto-dev.gnm.int:8080/API/storage/VX-3/file/$1 -u user:passwd -D-\n";
}