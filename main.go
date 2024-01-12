package main

import (
	"database/sql"
	"flag"
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
	defHost       = "127.0.0.1"
	defPort       = "4000"
	defUser       = "root"
	defPassword   = ""
	defSchemaName = "simplebench"
	defTableName  = "combinedPK"
	characters    = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ01234567890!@#$^&*()-_=+,.?/:;<>{}|"
	tableRows     = 10000000
	days          = 900
)

var schemaName, tableName, user, password *string
var selectConcurrency *uint
var selectDuration *time.Duration

type flagHosts []string

var dbHosts flagHosts

func (h *flagHosts) String() string {
	return strings.Join(*h, ",")
}

func (h *flagHosts) Set(s string) error {
	if strings.Contains(s, ",") {
		for _, v := range strings.Split(s, ",") {
			*h = append(*h, v)
		}
	} else {
		*h = append(*h, s)
	}
	return nil
}

func randomChars(num int, sb *strings.Builder) error {
	for j := 0; j < num; j++ {
		err := sb.WriteByte(characters[rand.Intn(len(characters))])
		if err != nil {
			return err
		}
	}
	return nil
}

func insert(start, end uint, host string) error {
	dbConnectString := fmt.Sprintf("%s:%s@tcp(%s)/%s", *user, *password, host, *schemaName)
	db, err := sql.Open("mysql", dbConnectString)
	if err != nil {
		return err
	}
	batchSize := uint(1000)
	var sb strings.Builder
	for ; start < end; start += batchSize {
		sb.Reset()
		sb.WriteString("insert into `" + *tableName + "` values ")
		for i := uint(0); i < batchSize; i++ {
			if i != 0 {
				sb.WriteString(",")
			}
			id := uint32(start + i)
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
			t := time.Now().AddDate(0, 0, 0-rand.Intn(days)).Add(-d)
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

func insertRoutine(start, end uint, host string, ch chan error) {
	err := insert(start, end, host)
	ch <- err
}

func parallelInsert(start, end uint) (err error) {
	ch := make(chan error, 3)
	routines := uint(10)
	size := (end - start) / routines
	concurrentInserts := 0
	t := time.Now()
	fmt.Printf("Starting insert: %s\n", t.Format(time.RFC3339))
	for i := start; i < end; i += size {
		host := dbHosts[concurrentInserts%len(dbHosts)]
		fmt.Printf("Inserting %d < %d on host %s\n", i, i+size, host)
		go insertRoutine(i, i+size, host, ch)
		concurrentInserts++
	}
	for i := 0; i < concurrentInserts; i++ {
		fmt.Printf("Waiting for %d insert routines to end\n", concurrentInserts-i)
		err = <-ch
		if err != nil {
			break
		}
	}
	fmt.Printf("Done insert: %s %s\n", time.Now().Format(time.RFC3339), time.Now().Sub(t).String())
	return err
}

type report struct {
	queries    uint
	totalTime  time.Duration
	minLatency time.Duration
	maxLatency time.Duration
}

func selectPK(start, end uint, host string, dur time.Duration, ch chan report) {
	dbConnectString := fmt.Sprintf("%s:%s@tcp(%s)/%s", *user, *password, host, *schemaName)
	db, err := sql.Open("mysql", dbConnectString)
	if err != nil {
		log.Fatalf("Could not connect to database: %v", err)
	}
	//fmt.Printf("Starting selects between %d and %d on host %s\n", start, end, host)
	var rep report
	startT := time.Now()
	reportT := startT
	// TODO: Should we really include all columns, especially the payload one?
	sqlStr := "select * from `" + *tableName + "` where id = "
	for {
		id := rand.Uint32()%uint32(end-start) + uint32(start)
		idStr := fmt.Sprintf("'%08x%08X%08x%08d'", bits.Reverse32(id), id^0x01234567, id^0x76543210, id)
		beforeT := time.Now()
		res, err := db.Query(sqlStr + idStr)
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

func runSelects(start, end uint) {
	// TODO: Increase number of threads, start by 1, the *2 until QPS is decreased 25% or threads > 2000
	ch := make(chan report, 10000)
	var wg sync.WaitGroup
	tStart := time.Now()
	if *selectConcurrency < 1 {
		*selectConcurrency = 100
	}
	for i := 0; i < int(*selectConcurrency); i++ {
		host := dbHosts[i%len(dbHosts)]
		wg.Add(1)
		go func() {
			selectPK(start, end, host, *selectDuration, ch)
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
		avgLat := int64(0)
		if rep.queries > 0 {
			avgLat = rep.totalTime.Microseconds() / int64(rep.queries)
		}
		fmt.Printf("c % 8d\tq % 8d\tt % 8d ms\tmin % 8d us\tmax % 8d us\tavg % 8d us\n",
			count, rep.queries, rep.totalTime.Milliseconds(), rep.minLatency.Microseconds(), rep.maxLatency.Microseconds(), avgLat)
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
		if tStart.Before(time.Now().Add(-*selectDuration)) && rep.queries == 0 {
			break
		}
	}
	totLat := int64(0)
	if totRep.queries > 0 {
		totLat = totRep.totalTime.Microseconds() / int64(totRep.queries)
	}
	fmt.Printf("T % 8d\tq % 8d\tt % 8d ms\tmin % 8d us\tmax % 8d us\tavg % 8d us\n",
		totCount, totRep.queries, totRep.totalTime.Milliseconds(), totRep.minLatency.Microseconds(), totRep.maxLatency.Microseconds(), totLat)
	wg.Wait()
	fmt.Printf("Selects all done!\n")
}

func main() {
	numPartitions := flag.Uint("parts", 0, "Number of partitions, 0 = non-partitioned table")
	createNewTable := flag.Bool("create", false, "Create the database and (re)create table")
	newRows := flag.Bool("insert", false, "insert -rows number of rows in the table")
	numRows := flag.Uint("rows", tableRows, "number of rows in the table")
	schemaName = flag.String("schema", defSchemaName, "Use this schema name")
	tableName = flag.String("table", defTableName, "Use this table name")
	doSelect := flag.Bool("select", false, "Run select PK benchmark")
	sleepTime := flag.Uint("sleep", 10, "Sleep this number of seconds between insert and select benchmark")
	pkCols := flag.String("pkcols", "id,ts", "Primary columns")
	user = flag.String("user", defUser, "database user name")
	password = flag.String("password", defPassword, "database user password")
	selectDuration = flag.Duration("duration", 60*time.Second, "Duration of select benchmark")
	selectConcurrency = flag.Uint("concurrency", 100, "number of concurrent selects")
	flag.Var(&dbHosts, "host", "database host:port, give multiple time or as a comma separated list")
	flag.Parse()

	if len(dbHosts) == 0 {
		dbHosts = append(dbHosts, defHost+":"+defPort)
	}
	var err error
	if *createNewTable {
		// (re)create database and table
		dbConnectString := fmt.Sprintf("%s:%s@tcp(%s)/", *user, *password, dbHosts[0])
		db, err := sql.Open("mysql", dbConnectString)
		if err != nil {
			log.Fatalf("Could not connect to database: %v", err)
		}
		if db == nil {
			log.Fatalln("Could not connect to database: db is nil")
		}

		// Populate the table
		/*
			_, err = db.Exec("drop schema if exists " + schema)
			if err != nil {
				log.Fatalf("Error cleaning up old schema simplebench: %v", err)
			}
		*/
		_, err = db.Exec("create schema if not exists " + *schemaName)
		if err != nil {
			log.Fatalf("Error creating schema simplebench: %v", err)
		}

		_, err = db.Exec("use " + *schemaName)
		if err != nil {
			log.Fatalf("Error using schema simplebench: %v", err)
		}

		_, err = db.Exec("drop table if exists `" + *tableName + "`")
		if err != nil {
			log.Fatalf("Error using schema simplebench: %v", err)
		}

		createSQL := "create table `" + *tableName + "`" + ` 
(id varchar(32) not null, -- actual PK
 a varchar(32) not null,
 b varchar(32) not null,
 c varchar(32) not null,
 d varchar(32) not null,
 e int not null,
 ts timestamp not null default current_timestamp,
 payload varchar(10240) not null,`
		createSQL += "\n PRIMARY KEY (" + *pkCols + "))"
		if *numPartitions > 0 {
			createSQL += "\npartition by range (unix_timestamp(ts))\n("
			interval := days / *numPartitions
			t := time.Now().Add(-days * time.Hour * 24)
			for i := uint(1); i < *numPartitions; i++ {
				if i > 1 {
					createSQL += " "
				}
				t = t.Add(time.Duration(interval) * 24 * time.Hour)
				dateStr := t.Format(time.DateOnly)
				createSQL += "partition `p" + dateStr + "` values less than (unix_timestamp('" + dateStr + "')),\n"
			}
			if *numPartitions == 1 {
				createSQL += " "
			}
			createSQL += "partition pMax values less than (maxvalue))"
		}
		fmt.Println(createSQL)
		_, err = db.Exec(createSQL)
		if err != nil {
			log.Fatalf("Error creating table: %v", err)
		}

		err = db.Close()
		if err != nil {
			log.Fatalf("Error closing database: %v", err)
		}
	}

	// spawn goroutines to run queries, including report qps and latency
	if *newRows {
		err = parallelInsert(0, *numRows)
		if err != nil {
			log.Fatalf("Error populating table: %v", err)
		}
	}

	if *doSelect {
		if *newRows {
			time.Sleep(time.Duration(*sleepTime) * time.Second)
		}
		runSelects(0, *numRows)
	}
}
