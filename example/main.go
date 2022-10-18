package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/go-god/timewheel"
)

func main() {
	// init timeWheel
	tw, err := timewheel.New(1*time.Second, 10,
		timewheel.WithName("test_time_wheel"),
		timewheel.WithLoggerFunc(log.Printf),
		// timewheel.WithSyncRunEachTask(),
		timewheel.WithPerBucketPreNum(100),
	)

	if err != nil {
		log.Fatalln(err)
	}

	// add normal task
	_ = tw.AddTask("abc", 1*time.Second, func(data interface{}) {
		log.Println(1111)
	})

	// add timed schedule task
	_ = tw.AddTask("12", 1*time.Second, func(data interface{}) {
		log.Println(12356)
		log.Println("data: ", data)
	}, timewheel.WithTaskSchedule(), timewheel.WithTaskData("abc"))

	// tw.RemoveTask("12")
	// time.Sleep(10 * time.Second) // or signal exit handler
	// tw.Stop() // stop timeWheel

	shutdown(tw, 5*time.Second)
}

// shutdown stop timeWheel
func shutdown(tw *timewheel.TimeWheel, wait time.Duration) {
	ch := make(chan os.Signal, 1)
	// We'll accept graceful shutdowns when quit via SIGINT (Ctrl+C)
	// recv signal to exit main goroutine
	// window signal
	signal.Notify(ch, syscall.SIGINT, syscall.SIGTERM, os.Interrupt, syscall.SIGHUP)

	// linux signal if you use linux on production,please use this code.
	// signal.Notify(ch, syscall.SIGINT, syscall.SIGTERM, syscall.SIGUSR2, os.Interrupt, syscall.SIGHUP)

	// Block until we receive our signal.
	sig := <-ch

	log.Println("exit signal: ", sig.String())
	// Create a deadline to wait for.
	ctx, cancel := context.WithTimeout(context.Background(), wait)
	defer cancel()

	tw.Stop()

	<-ctx.Done()
}
