#!/bin/bash

#awk '{
#    for(i = 1; i <= NF; i++) {
#        if ($i ~ /key:/) {
#            split($i, a, ":");
#            gsub(/,/, "", a[2]);
#            print a[2];
#        }
#    }
#}' a 

awk '{
    while(match($0, /key: [0-9]+/)) {
        print substr($0, RSTART + 5, RLENGTH - 5);
        $0 = substr($0, RSTART + RLENGTH);
    }
}' a | sort -n > x

