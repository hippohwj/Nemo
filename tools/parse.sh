#!/bin/bash
awk '{
    while(match($0, /pageid: [0-9]+/)) {
        print substr($0, RSTART + 8, RLENGTH - 8);
        $0 = substr($0, RSTART + RLENGTH);
    }
}' a2 | sort -n > ax
