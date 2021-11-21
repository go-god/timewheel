package timewheel

import (
	"errors"
	"sync"
	"time"
)

const tmFmtWithMS = "2006-01-02 15:04:05.999"

type status int8

const (
	ready status = iota + 1
	run
	stop
)

const (
	// max buckets
	maxBuckets = 1024 * 1024
)

var (
	// ErrIllegalInterval interval invalid
	ErrIllegalInterval = errors.New("illegal interval time")

	// ErrIllegalBucketNum bucket num invalid
	ErrIllegalBucketNum = errors.New("illegal bucket num")

	// ErrIllegalTaskDelay delay time invalid
	ErrIllegalTaskDelay = errors.New("illegal delay time")

	// ErrTaskKeyExist task key already exist
	ErrTaskKeyExist = errors.New("task key already exists")

	// ErrNotRun timeWheel not run
	ErrNotRun = errors.New("timeWheel not running")
)

// TimeWheel time wheel
type TimeWheel struct {
	name            string
	ticker          *time.Ticker
	interval        time.Duration
	bucketNum       int
	buckets         []*bucket
	perBucketPreNum int
	curPos          int
	keyPosMap       sync.Map
	status          status
	once            sync.Once
	stopChan        chan struct{}
	done            chan struct{}
	recovery        func()
	logger          Logger
	enableShutdown  bool
	wg              *sync.WaitGroup
}

type bucket struct {
	list []*task
	mu   sync.Mutex
}

func (b *bucket) push(t *task) {
	b.mu.Lock()
	b.list = append(b.list, t)
	b.mu.Unlock()
}

// Callback task callback
type Callback func(data interface{})

type task struct {
	key      string
	delay    time.Duration
	pos      int
	circle   int
	fn       Callback
	data     interface{}
	schedule bool // timed schedule task
}

// New create a timeWheel
func New(interval time.Duration, bucketsNum int, opts ...Option) (*TimeWheel, error) {
	if interval <= 0 {
		return nil, ErrIllegalInterval
	}

	if bucketsNum <= 0 {
		return nil, ErrIllegalBucketNum
	}

	num := normalizeTicksPerWheel(bucketsNum)
	tw := &TimeWheel{
		ticker:          time.NewTicker(interval),
		interval:        interval,
		bucketNum:       num,
		buckets:         make([]*bucket, num),
		perBucketPreNum: 200,
		curPos:          0,
		status:          ready,
		stopChan:        make(chan struct{}, 1),
		done:            make(chan struct{}, 1),
		logger:          NewNopLogger(),
	}

	for _, o := range opts {
		o(tw)
	}

	if tw.recovery == nil {
		tw.recovery = tw.defaultRecovery
	}

	tw.initBuckets()

	if tw.enableShutdown {
		tw.wg = &sync.WaitGroup{}
	}

	// start the timeWheel
	tw.status = run
	go tw.start()

	return tw, nil
}

func (tw *TimeWheel) initBuckets() {
	for i := 0; i < tw.bucketNum; i++ {
		tw.buckets[i] = &bucket{list: make([]*task, 0, tw.perBucketPreNum)}
	}
}

func (tw *TimeWheel) defaultRecovery() {
	if err := recover(); err != nil {
		tw.logger.Printf("exec recover error:%v", err)
	}
}

// AddTask add ordinary delay task
func (tw *TimeWheel) AddTask(key string, delay time.Duration, fn Callback, opts ...TaskOption) error {
	return tw.addTask(key, delay, fn, opts...)
}

func (tw *TimeWheel) addTask(key string, delay time.Duration, fn Callback, opts ...TaskOption) error {
	if tw.status != run {
		return ErrNotRun
	}

	if _, exist := tw.keyPosMap.Load(key); exist {
		return ErrTaskKeyExist
	}

	if delay <= 0 {
		return ErrIllegalTaskDelay
	}

	if delay < tw.interval {
		delay = tw.interval
	}

	pos, circle := tw.getPositionAndCircle(delay)
	taskEntry := &task{
		delay:  delay,
		key:    key,
		pos:    pos,
		circle: circle,
		fn:     fn,
	}

	for _, o := range opts {
		if o == nil {
			continue
		}

		o(taskEntry)
	}

	tw.buckets[pos].push(taskEntry)
	tw.keyPosMap.Store(key, pos)
	return nil
}

// RemoveTask remove task by key
func (tw *TimeWheel) RemoveTask(key string) {
	pos, ok := tw.keyPosMap.Load(key)
	if !ok {
		return
	}

	bucketEntry := tw.buckets[pos.(int)]
	bucketEntry.mu.Lock()
	defer bucketEntry.mu.Unlock()

	for i := 0; i < len(bucketEntry.list); i++ {
		t := bucketEntry.list[i]
		if key == t.key {
			bucketEntry.list = append(bucketEntry.list[:i], bucketEntry.list[i+1:]...)
			tw.keyPosMap.Delete(key)
			return
		}
	}
}

func (tw *TimeWheel) getPositionAndCircle(delay time.Duration) (pos int, circle int) {
	dd := int(delay / tw.interval)
	circle = dd / tw.bucketNum

	// the tw.curPos here means that the tasks in the position have been processed
	pos = (tw.curPos + dd) & (tw.bucketNum - 1)
	if circle > 0 && pos == tw.curPos {
		circle--
	}

	return
}

// Stop stop timeWheel
func (tw *TimeWheel) Stop() {
	tw.once.Do(func() {
		tw.logger.Printf("timeWheel %s recv stop action", tw.name)
		close(tw.stopChan)
		tw.status = stop
		<-tw.done
		tw.logger.Printf("timeWheel %s exit success", tw.name)
	})
}

func (tw *TimeWheel) start() {
	defer tw.recovery()
	tw.logger.Printf("timeWheel %s start at:%s", tw.name, time.Now().Format(tmFmtWithMS))

	for {
		select {
		case <-tw.ticker.C:
			tw.handleTicker()
		case <-tw.stopChan:
			tw.ticker.Stop()
			if tw.enableShutdown {
				tw.logger.Printf("timeWheel %s will shutdown...", tw.name)
				tw.wg.Wait()
			}

			close(tw.done)
			return
		}
	}
}

func (tw *TimeWheel) handleTicker() {
	now := time.Now()
	curPos := (tw.curPos + 1) & (tw.bucketNum - 1) // equals (tw.curPos + 1) % tw.bucketNum
	bucketEntry := tw.buckets[curPos]
	bucketEntry.mu.Lock()
	defer bucketEntry.mu.Unlock()

	tw.curPos = curPos
	k := 0
	execNum := len(bucketEntry.list)
	for i := 0; i < execNum; i++ {
		taskEntry := bucketEntry.list[i]
		if taskEntry.circle > 0 {
			taskEntry.circle--
			bucketEntry.list[k] = taskEntry
			k++
			continue
		}

		if tw.enableShutdown {
			tw.wg.Add(1)
		}

		go func() {
			defer tw.recovery()
			if tw.enableShutdown {
				defer tw.wg.Done()
			}

			taskEntry.fn(taskEntry.data)
		}()

		if !taskEntry.schedule {
			tw.keyPosMap.Delete(taskEntry.key)
			continue
		}

		// timed schedule task
		_, ok := tw.keyPosMap.Load(taskEntry.key)
		if !ok {
			continue
		}

		pos, circle := tw.getPositionAndCircle(taskEntry.delay) // reload
		taskEntry.pos = pos
		taskEntry.circle = circle
		if pos == curPos {
			bucketEntry.list[k] = taskEntry
			k++
		} else {
			tw.buckets[pos].push(taskEntry)
			tw.keyPosMap.Store(taskEntry.key, pos)
		}
	}

	bucketEntry.list = bucketEntry.list[:k]
	end := time.Now()
	tw.logger.Printf("timeWheel %s task begin time: %d, end time: %d, cost time: %v, exec num: %d",
		tw.name, now.Unix(), end.Unix(), end.Sub(now), execNum)
}

// get the bucket capacity
func normalizeTicksPerWheel(ticksPerWheel int) int {
	u := ticksPerWheel - 1
	u |= u >> 1
	u |= u >> 2
	u |= u >> 4
	u |= u >> 8
	u |= u >> 16
	if u+1 > maxBuckets {
		return maxBuckets
	}

	return u + 1
}
