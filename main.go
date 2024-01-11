package main

import (
	"database/sql"
	"fmt"
	_ "github.com/go-sql-driver/mysql"
	"log"
	"math/bits"
	"math/rand"
	"strings"
	"sync"
	"time"
)

const (
	host      = "127.0.0.1"
	port      = 4000
	user      = "root"
	password  = ""
	schema    = "simplebench"
	tableName = "combinedPK"
	//characters = "abcdefghijklmnopqrstuvwxyz"
	characters = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ01234567890!@#$^&*()-_=+,.?/:;<>{}|"
	tableRows  = 10000000
)

func randomChars(num int, sb *strings.Builder) error {
	for j := 0; j < num; j++ {
		err := sb.WriteByte(characters[rand.Intn(len(characters))])
		if err != nil {
			return err
		}
	}
	return nil
}

func insert(start, end uint32) error {
	dbConnectString := fmt.Sprintf("%s:%s@tcp(%s:%d)/%s", user, password, host, port, schema)
	db, err := sql.Open("mysql", dbConnectString)
	if err != nil {
		return err
	}
	batchSize := uint32(1000)
	var sb strings.Builder
	for ; start < end; start += batchSize {
		sb.Reset()
		sb.WriteString("insert into " + tableName + " values ")
		for i := uint32(0); i < batchSize; i++ {
			if i != 0 {
				sb.WriteString(",")
			}
			id := start + i
			_, err = fmt.Fprintf(&sb, "('%08x%08X%08x%08d'",
				bits.Reverse32(id), id^0x01234567, id^0x76543210, id)
			if err != nil {
				return err
			}
			for f := 0; f < 4; f++ {
				_, err = sb.WriteString(",'")
				if err != nil {
					return err
				}
				err = randomChars(32, &sb)
				if err != nil {
					return err
				}
				_, err = sb.WriteString("'")
				if err != nil {
					return err
				}
			}
			d := time.Duration(rand.Intn(86400)) * time.Second
			t := time.Now().AddDate(0, 0, 0-rand.Intn(900)).Add(-d)
			_, err = fmt.Fprintf(&sb, ",%d,'%s','", i, t.Format(time.DateTime))
			if err != nil {
				return err
			}
			err = randomChars(856, &sb)
			if err != nil {
				return err
			}
			_, err = sb.WriteString("')")
		}
		//fmt.Printf("generated insert query:\nvvvvvvv\n%s\n^^^^^^^\n", sb.String())
		_, err = db.Exec(sb.String())
		if err != nil {
			return err
		}
		//fmt.Printf("%d,", start/batchSize)
	}
	return nil
}

func insertRoutine(start, end uint32, ch chan error) {
	err := insert(start, end)
	ch <- err
}

func parallelInsert(start, end uint32) (err error) {
	ch := make(chan error, 3)
	routines := uint32(10)
	size := (end - start) / routines
	concurrentInserts := 0
	t := time.Now()
	fmt.Printf("Starting insert: %s", t.Format(time.RFC3339))
	for i := start; i < end; i += size {
		fmt.Printf("Inserting %d < %d\n", i, i+size)
		go insertRoutine(i, i+size, ch)
		concurrentInserts++
	}
	for i := 0; i < concurrentInserts; i++ {
		fmt.Printf("Waiting for %d insert routines to end\n", concurrentInserts-i)
		err = <-ch
		if err != nil {
			break
		}
	}
	fmt.Printf("Done insert: %s\nDuration: %s", time.Now().Format(time.RFC3339), t.Sub(time.Now()).String())
	return err
}

type report struct {
	queries    uint32
	totalTime  time.Duration
	minLatency time.Duration
	maxLatency time.Duration
}

func selectPK(start, end uint32, dur time.Duration, ch chan report) {
	dbConnectString := fmt.Sprintf("%s:%s@tcp(%s:%d)/%s", user, password, host, port, schema)
	db, err := sql.Open("mysql", dbConnectString)
	if err != nil {
		log.Fatalf("Could not connect to database: %v", err)
	}
	var rep report
	startT := time.Now()
	reportT := startT
	// TODO: Should we really include all columns, especially the payload one?
	sql := "select * from " + tableName + " where id = "
	for {
		id := rand.Uint32()%(end-start) + start
		idStr := fmt.Sprintf("'%08x%08X%08x%08d'", bits.Reverse32(id), id^0x01234567, id^0x76543210, id)
		beforeT := time.Now()
		res, err := db.Query(sql + idStr)
		if err != nil {
			log.Fatalf("Could not select: %v", err)
		}
		afterT := time.Now()
		found := false
		for res.Next() {
			// Skip using the result...
			if found {
				log.Fatalf("Duplicate key! %v", idStr)
			}
			found = true
		}
		if !found {
			log.Fatalf("Missing key! %v", idStr)
		}
		err = res.Close()
		if err != nil {
			log.Fatalf("Could not close select: %v", err)
		}
		d := afterT.Sub(beforeT)
		rep.totalTime += d
		if rep.queries == 0 || rep.minLatency > d {
			rep.minLatency = d
		}
		if rep.maxLatency < d {
			rep.maxLatency = d
		}
		rep.queries++
		//fmt.Printf("S")
		if reportT.Before(afterT.Add(-100 * time.Millisecond)) {
			ch <- rep
			rep.queries = 0
			rep.totalTime = 0
			rep.maxLatency = 0
			rep.minLatency = 0
			reportT = reportT.Add(time.Second)
		}
		if startT.Add(dur).Before(afterT) {
			//fmt.Printf("a")
			break
		}
		//fmt.Printf("s")
	}
	//fmt.Printf("q")
	if rep.queries > 0 {
		ch <- rep
	}
}

func runSelects(start, end uint32) {
	// TODO: Increase number of threads, start by 1, the *2 until QPS is decreased 25% or threads > 2000
	concurrency := 100
	dur := 5 * 60 * time.Second
	ch := make(chan report, 10000)
	var wg sync.WaitGroup
	tStart := time.Now()
	for i := 0; i < concurrency; i++ {
		wg.Add(1)
		go func() {
			selectPK(start, end, dur, ch)
			wg.Done()
		}()
	}
	fmt.Println("Started all selects")
	var totRep report
	totCount := 0
	for range time.Tick(time.Second) {
		var rep report
		count := 0
		//fmt.Printf("T")
		for {
			stop := false
			select {
			case r := <-ch:
				if count == 0 || r.minLatency < rep.minLatency {
					if r.minLatency > 0 {
						rep.minLatency = r.minLatency
					}
				}
				if r.maxLatency > rep.maxLatency {
					rep.maxLatency = r.maxLatency
				}
				rep.queries += r.queries
				rep.totalTime += r.totalTime
				count++
			default:
				stop = true
				break
			}
			if stop {
				break
			}
		}
		fmt.Printf("c % 8d\tq % 8d\tt % 8d ms\tmin % 8d us\tmax % 8d us\n",
			count, rep.queries, rep.totalTime.Milliseconds(), rep.minLatency.Microseconds(), rep.maxLatency.Microseconds())
		if totRep.queries == 0 || totRep.minLatency > rep.minLatency {
			if rep.minLatency > 0 {
				totRep.minLatency = rep.minLatency
			}
		}
		if totRep.maxLatency < rep.maxLatency {
			totRep.maxLatency = rep.maxLatency
		}
		totRep.queries += rep.queries
		totRep.totalTime += rep.totalTime
		totCount += count
		if tStart.Before(time.Now().Add(-dur - 10*time.Second)) {
			break
		}
	}
	fmt.Printf("T % 8d\tq % 8d\tt % 8d ms\tmin % 8d us\tmax % 8d us\n",
		totCount, totRep.queries, totRep.totalTime.Milliseconds(), totRep.minLatency.Microseconds(), totRep.maxLatency.Microseconds())
	wg.Wait()
	fmt.Printf("Selects all done!\n")
}

func main() {
	var err error
	if true {
		// (re)create database and table
		dbConnectString := fmt.Sprintf("%s:%s@tcp(%s:%d)/", user, password, host, port)
		db, err := sql.Open("mysql", dbConnectString)
		if err != nil {
			log.Fatalf("Could not connect to database: %v", err)
		}
		if db == nil {
			log.Fatalln("Could not connect to database: db is nil")
		}

		// Populate the table
		_, err = db.Exec("drop schema if exists " + schema)
		if err != nil {
			log.Fatalf("Error cleaning up old schema simplebench: %v", err)
		}
		_, err = db.Exec("create schema " + schema)
		if err != nil {
			log.Fatalf("Error creating schema simplebench: %v", err)
		}

		_, err = db.Exec("use " + schema)
		if err != nil {
			log.Fatalf("Error using schema simplebench: %v", err)
		}

		createSQL := "create table " + tableName + ` 
(id varchar(32) not null, -- actual PK
 a varchar(32) not null,
 b varchar(32) not null,
 c varchar(32) not null,
 d varchar(32) not null,
 e int not null,
 ts timestamp not null default current_timestamp,
 payload varchar(10240) not null,
		PRIMARY KEY (id,ts))`
		//PRIMARY KEY (id))`
		if true {
			partitionBy := ` partition by range (unix_timestamp(ts))
(partition p2021 values less than (unix_timestamp('2022-01-01')),
 partition p2022 values less than (unix_timestamp('2023-01-01')),
 partition p2023 values less than (unix_timestamp('2024-01-01')),
 partition p2024 values less than (unix_timestamp('2025-01-01')))`
			fmt.Println(createSQL + partitionBy)
			_, err = db.Exec(createSQL + partitionBy)
		} else {
			fmt.Println(createSQL)
			_, err = db.Exec(createSQL)
		}
		if err != nil {
			log.Fatalf("Error creating table: %v", err)
		}

		err = db.Close()
		if err != nil {
			log.Fatalf("Error closing database: %v", err)
		}
	}

	// spawn goroutines to run queries, including report qps and latency
	err = parallelInsert(0, tableRows)
	if err != nil {
		log.Fatalf("Error populating table: %v", err)
	}

	time.Sleep(300 * time.Second)
	runSelects(0, tableRows)
}
