package timewheel

// Option time wheel option
type Option func(tw *TimeWheel)

// WithName time wheel time
func WithName(name string) Option {
	return func(tw *TimeWheel) {
		tw.name = name
	}
}

// WithRecoveryFunc recovery func
func WithRecoveryFunc(fn func()) Option {
	return func(tw *TimeWheel) {
		tw.recovery = fn
	}
}

// WithLogger set time wheel logger
func WithLogger(logger Logger) Option {
	return func(tw *TimeWheel) {
		tw.logger = logger
	}
}

// WithLoggerFunc set time wheel logger func
func WithLoggerFunc(fn func(msg string, args ...interface{})) Option {
	return func(tw *TimeWheel) {
		tw.logger = LoggerFunc(fn)
	}
}

// WithSyncRunEachTask sync run each task in goroutine
func WithSyncRunEachTask() Option {
	return func(tw *TimeWheel) {
		tw.syncRunEachTask = true
	}
}

// WithPerBucketPreNum set per bucket pre cap
func WithPerBucketPreNum(num int) Option {
	return func(tw *TimeWheel) {
		tw.perBucketPreNum = num
	}
}

// TaskOption task option
type TaskOption func(task *task)

// WithTaskData set task data
func WithTaskData(data interface{}) TaskOption {
	return func(task *task) {
		task.data = data
	}
}

// WithTaskSchedule set schedule task
func WithTaskSchedule() TaskOption {
	return func(task *task) {
		task.schedule = true
	}
}
