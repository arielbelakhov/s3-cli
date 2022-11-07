package main

import (
	"flag"
	"fmt"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
	"gorm.io/driver/postgres"
	"gorm.io/gorm"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"
)

// customer_id, loc_id, device_id, from-to time, --bucket
//--aws-creds

func main() {
	startTime := time.Now()
	fromTime := flag.String("from", "", "from time (recording_ts) format: '2022-05-22 21:30:00'")
	toTime := flag.String("to", "", "end time (recording_ts) format: '2022-05-22 21:30:00'")
	bucket := flag.String("bucket", "", "aws bucket name")
	host := flag.String("host", "172.31.1.116", "postgres host name")
	port := flag.Int("port", 6432, "postgres port")
	user := flag.String("user", "", "postgres user")
	password := flag.String("password", "", "postgres password")
	dbname := flag.String("dbname", "", "postgres dbname")
	customerId := flag.Int("customer_id", 0, "customer id (optional)")
	deviceId := flag.Int("device_id", 0, "device id (optional)")
	pathToSave := flag.String("path", "", "directory to save to")
	flag.Parse()

	if *pathToSave == "" || *dbname == "" || *bucket == "" || *fromTime == "" || *toTime == "" {
		exitErrorf("not enough flags, use --help")
	}

	maxGoroutines := 400
	dsn := fmt.Sprintf("host=%s user=%s password=%s dbname=%s port=%d sslmode=disable", *host, *user, *password, *dbname, *port)
	db, err := gorm.Open(postgres.Open(dsn), &gorm.Config{PrepareStmt: false})
	if err != nil {
		exitErrorf("Error while connecting to Postgres, %s", err)
	}
	var validKeys []string

	tableName := "cl_full_segments"
	if strings.ToLower(*bucket) == "clanz-uploads-ogg" {
		db = db.Table(tableName)
		var s3Links []string

		db = db.Select("cl_full_segments.s3_link").Where("recording_ts between ? and ?", *fromTime, *toTime)
		if *deviceId != 0 {
			db = db.Where("device_id = ?", *deviceId)
		}
		if *customerId != 0 {
			db = db.Where("customer_id = ?", *customerId)
		}

		err = db.Scan(&s3Links).Error

		if err != nil {
			exitErrorf("Error while query Postgres: %s", err)
		}
		for _, key := range s3Links {
			index := strings.Index(key, ".com")
			validKeys = append(validKeys, key[index+5:])
		}

	} else {
		tableName = "cl_tagging_results"
		db = db.Table(tableName)

		type A struct {
			FileName    string    `gorm:"column:file_name"`
			RecordingTs time.Time `gorm:"column:recording_ts"`
			Uid         string    `gorm:"column:uid"`
		}

		var s3Links []A

		db = db.Select("file_name, cl_devices.uid, recording_ts").
			Joins("inner join cl_devices on cl_devices.id = cl_tagging_results.device_id").
			Where("recording_ts between ? and ?", *fromTime, *toTime)

		if *deviceId != 0 {
			db = db.Where("device_id = ?", *deviceId)
		}
		if *customerId != 0 {
			db = db.Where("customer_id = ?", *customerId)
		}

		err = db.Scan(&s3Links).Error

		if err != nil {
			exitErrorf("Error while query Postgres: %s", err)
		}
		const layout = "2006/01/02"
		const layout2 = "16"

		for _, key := range s3Links {
			//fmt.Println(, 4)
			hour := key.RecordingTs.Hour()
			var hourStr string
			if hour < 10 {
				hourStr = fmt.Sprintf("0%d", hour)
			} else {
				hourStr = strconv.Itoa(hour)
			}

			link := fmt.Sprintf("clanz-3reality-%s/%s/%s/%s",
				key.Uid, key.RecordingTs.Format(layout), hourStr, key.FileName)
			validKeys = append(validKeys, link)
		}
	}

	// credentials from the shared credentials file ~/.aws/credentials.
	sess, _ := session.NewSession(&aws.Config{
		Region: aws.String("eu-west-1")},
	)

	downloader := s3manager.NewDownloader(sess)

	if err != nil {
		exitErrorf("Concurrency limit required")
	}

	guard := make(chan int, maxGoroutines)
	wg := sync.WaitGroup{}

	fmt.Printf("downloading %d files", len(validKeys))
	for _, key := range validKeys {
		guard <- 1
		wg.Add(1)
		go func(key string) {
			index := strings.LastIndex(key, "/")
			_ = os.Mkdir(*pathToSave, os.ModePerm)
			file, err := os.Create(fmt.Sprintf("./%s/%s", *pathToSave, key[index+1:]))
			if err != nil {
				exitErrorf("Unable to open file %q, %v", key, err)
			}
			defer file.Close()
			defer wg.Done()

			numBytes, err := downloader.Download(file,
				&s3.GetObjectInput{
					Bucket: aws.String(*bucket),
					Key:    aws.String(key),
				})
			if err != nil {
				println("Unable to download item %q, %v", key, err)
			}

			fmt.Println("Downloaded", file.Name(), numBytes, "bytes")

			<-guard
		}(key)
	}

	wg.Wait()
	fmt.Printf("took: %f seconds\n", time.Since(startTime).Seconds())
}

func exitErrorf(msg string, args ...interface{}) {
	fmt.Fprintf(os.Stderr, msg+"\n", args...)
	os.Exit(1)
}
