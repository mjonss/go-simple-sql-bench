time ./go-simple-sql-bench -rows 10000000 -create -insert -host "127.0.0.1:4000" -host "127.0.0.1:4001" -table nonpart
sleep 120
time ./go-simple-sql-bench -rows 10000000 -create -insert -host "127.0.0.1:4000" -host "127.0.0.1:4001" -table part_01 -parts 1
sleep 120
time ./go-simple-sql-bench -rows 10000000 -create -insert -host "127.0.0.1:4000" -host "127.0.0.1:4001" -table part_02 -parts 2
sleep 120
time ./go-simple-sql-bench -rows 10000000 -create -insert -host "127.0.0.1:4000" -host "127.0.0.1:4001" -table part_06 -parts 6
sleep 120
time ./go-simple-sql-bench -rows 10000000 -host "127.0.0.1:4000" -host "127.0.0.1:4001" -select --duration 5m -table nonpart
sleep 120
time ./go-simple-sql-bench -rows 10000000 -host "127.0.0.1:4000" -host "127.0.0.1:4001" -select --duration 5m -table part_01
sleep 120
time ./go-simple-sql-bench -rows 10000000 -host "127.0.0.1:4000" -host "127.0.0.1:4001" -select --duration 5m -table part_02
sleep 120
time ./go-simple-sql-bench -rows 10000000 -host "127.0.0.1:4000" -host "127.0.0.1:4001" -select --duration 5m -table part_06
