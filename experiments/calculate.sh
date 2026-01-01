grep -oP 'latency:\s+\K[\d.]+' test01 | awk '
    { sum += $1 }               # $1 is the number output by grep
    END { printf "%.2f us\n", sum/NR }
'
