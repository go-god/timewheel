package timewheel

import (
	"log"
	"testing"
	"time"
)

func TestTimeWheel_AddTask(t *testing.T) {
	tw, err := New(1*time.Second, 10,
		WithName("test_time_wheel"),
		WithLoggerFunc(log.Printf),
		WithWaitAllTasksFinished(),
		WithPerBucketPreNum(100),
	)

	if err != nil {
		log.Fatalln(err)
	}

	_ = tw.AddTask("abc", 1*time.Second, func(data interface{}) {
		log.Println(1111)
	})

	_ = tw.AddTask("12", 1*time.Second, func(data interface{}) {
		log.Println(12356)
		log.Println("data: ", data)
	}, WithTaskSchedule(), WithTaskData("abc"))

	// tw.RemoveTask("12")
	time.Sleep(10 * time.Second)
	tw.Stop()
}
