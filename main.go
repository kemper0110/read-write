package main

import (
	"bufio"
	"fmt"
	"github.com/sirupsen/logrus"
	"github.com/spf13/viper"
	"github.com/thoas/go-funk"
	"os"
	"strconv"
	"sync"
	"time"
)

var overallSize []int64

func main() {
	size, threads, err := loadConfig()
	if err != nil {
		logrus.Fatalf("Config error: %s", err.Error())
	}
	overallSize = make([]int64, threads)

	//for i := range buffers {
	//	buffers[i] = make([]byte, 1024*1024)
	//}

	waitGroup := sync.WaitGroup{}
	waitGroup.Add(int(threads))

	startTogether := sync.WaitGroup{}
	startTogether.Add(1)

	work := func(id int64) {
		defer waitGroup.Done()
		file, err := os.Create(fmt.Sprintf("file%d.bin", id))
		if err != nil {
			logrus.Fatalf("create error: %s", err.Error())
		}
		defer func(file *os.File) {
			err := file.Close()
			if err != nil {
				logrus.Fatalf("closing error: %s", err.Error())
			}
		}(file)
		w := bufio.NewWriter(file)
		buffer := make([]byte, 1024*1024) // one megabyte buffer
		startTogether.Wait()
		for i := int64(0); i < size; i++ {
			writtenBytes, err := w.Write(buffer)
			if err != nil {
				logrus.Fatalf("write error: %s", err.Error())
			}
			overallSize[id] += int64(writtenBytes)
		}
	}
	for i := int64(0); i < threads; i++ {
		go work(i)
	}

	timerBegin := time.Now()

	startTogether.Done()
	waitGroup.Wait()

	timerEnd := time.Now()

	elapsed := timerEnd.Sub(timerBegin)
	bytesWritten := funk.SumInt64(overallSize)
	speed := float64(bytesWritten) / 1024 / 1024 / elapsed.Seconds()
	fmt.Printf("written %d bytes in %fs\n", bytesWritten, elapsed.Seconds())
	fmt.Printf("speed is %f mb/s", speed)
}

func loadConfig() (size int64, threads int64, err error) {
	viper.AddConfigPath("configs")
	viper.SetConfigName("config")
	err = viper.ReadInConfig()
	if err != nil {
		return
	}
	size, err = strconv.ParseInt(viper.GetString("size"), 10, 64)
	if err != nil {
		return
	}
	threads, err = strconv.ParseInt(viper.GetString("threads"), 10, 64)
	return
}
