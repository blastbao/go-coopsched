// Package coopsched is a benchmark and playground for https://github.com/golang/go/issues/51071.
package coopsched

import (
	"context"
	"errors"
	"runtime"
	"sync"
	"sync/atomic"
	"time"
)

// A Scheduler can manage a set of goroutines started with Go. If
// Yield is called, the goroutine may beblocked until the scheduler
// unblocks it. Yield blocks if the time slot is up, but is otherwise
// a no-op.
//
// Scheduler 可以管理一组用 Go 启动的 goroutine 。
//
// 如果调用了 Yield ，goroutine 可能会被阻塞，直到调度器将其解除阻塞。
// 如果时间片已好惊，就会调用 Yield 阻塞。
//
type Scheduler struct {
	algo SchedulingAlgo

	yieldCh chan *task
	doneCh  chan struct{}
	wg      sync.WaitGroup

	conc       uintptr // Configured number of running goroutines.
	numRunning uintptr // Actual number of running goroutines.
	timeSlot   uintptr // The currently executing time slot.

	blockingTimeNS int64
	runningTimeNS  int64
	waitingTimeNS  int64

	//
	sumQueued      int // Sum of the number of queued tasks for each Get.
	// 被唤醒的总数
	numGetCalls    int // The number of successful Get calls.
}

// NewScheduler creates a new scheduler with the given algorithm and
// concurrency.
func NewScheduler(conc int, algo SchedulingAlgo) *Scheduler {

	// 并发度，默认 cpu 核数
	if conc <= 0 {
		conc = runtime.GOMAXPROCS(0) - 1
		if conc <= 0 {
			conc = 1
		}
	}

	// 调度器
	s := &Scheduler{
		algo:    algo,
		yieldCh: make(chan *task, runtime.GOMAXPROCS(0)),
		doneCh:  make(chan struct{}),
		conc:    uintptr(conc),
	}

	// 启动两个后台协程
	s.wg.Add(2)
	go s.runQueue(newTaskPriorityQueue())
	go s.runTimeSlot()

	return s
}

// SchedulingAlgo is an algorithm for ordering tasks when scheduling
// them. A lower return value indicates a higher priority.
//
// SchedulingAlgo 是一种在调度任务时对任务进行排序的算法。
// 返回值越小，优先级越高。
type SchedulingAlgo func(t *task) int64


// FIFO selects the task that has waited the longest in the
// queue. This is what the Go scheduler (runq) does now.
//
// FIFO 会选择在队列中等待时间最长的任务。
// 这是 Go 现在所采用的调度方法。
func FIFO(t *task) int64 { return t.startNS }


// Waitiness orders tasks after the proportion of time spent
// waiting. It prefers tasks with more waiting than running. It
// essentially encodes a priority tuple like
//
//   (is-new, bucket(wait / (wait + run)), time-slot-age)
//
// into an int64. "is-new" becomes the sign. The age is used to create
// a (coarse) FIFO witih each bucket. The timing bucket occupies the
// top `factorBits" and the age the remaining bits.
//
// Waitiness 是按照等待时间的比例来安排任务。它更倾向于等待多于运行的任务。
//
//
//
//
//
func Waitiness(t *task) int64 {

	const (
		factorBits = 15
		ageBits    = 63 - factorBits

		factorMax = 1<<factorBits - 1
		ageMax    = 1<<ageBits - 1

		// factorHighBucketWidth is the fuzz-factor for CPU-intensive
		// tasks. The value 8 is loosely derived from cpuworker.go,
		// while it has a slightly different interpretation. Range [0,
		// factorMax].
		factorHighBucketWidth = factorMax / 8
	)


	if t.timeSlot == 0 {
		// New tasks are FIFOd with highest priority.
		return -int64(ageMax - atomic.LoadUintptr(&t.s.timeSlot))
	}

	rtNS := atomic.LoadInt64(&t.runningTimeNS)
	wtNS := atomic.LoadInt64(&t.waitingTimeNS)
	factor := factorMax * rtNS / (wtNS + rtNS) // Range [0, factorMax].
	if factor >= factorMax-factorHighBucketWidth {
		// This is a CPU-intensive task. This rounding increases load
		// in that bucket of the queue, making more tasks become
		// FIFO-scheduled together.
		factor = factorMax
	}

	// Within each waitiness factor bucket, use a FIFO.
	return (factor << (63 - factorBits)) | int64(t.timeSlot)
}

// Close stops the scheduler's internal goroutines, but does not stop
// goroutines started by Go. Yield panics if called after this
// function has been called.
//
// Close 会停止调度器的内部 goroutines ，但不会停止 Go 启动的 goroutines 。
// 在调用 Close 后再调用 Yield ，会出现 Panic 。
func (s *Scheduler) Close() error {
	close(s.yieldCh)
	close(s.doneCh)
	s.wg.Wait()

	return nil
}

// Do creates a new goroutine, managed by the scheduler. There's
// nothing special about the goroutine unless Yield is called.
//
// Do 创建一个由 s 负责调度的 goroutine 。
// 除非 Yield 被调用，否则这个 goroutine 没有什么特别之处。
func (s *Scheduler) Do(ctx context.Context, f func(context.Context)) {

	// 创建 task
	t := &task{
		s:       s,
		wakeCh:  make(chan struct{}, 1),
		startNS: nowNano(),
	}

	// 运行数 +1
	atomic.AddUintptr(&s.numRunning, 1)

	// 退出处理
	defer func() {
		// 更新累计运行时间
		t.runningTimeNS += nowNano() - t.startNS

		close(t.wakeCh)

		// 运行数 -1
		atomic.AddUintptr(&s.numRunning, ^uintptr(0))

		s.yieldCh <- nil

		// 更新阻塞时间、运行时间、等待时间
		atomic.AddInt64(&s.blockingTimeNS, t.blockingTimeNS)
		atomic.AddInt64(&s.runningTimeNS, t.runningTimeNS)
		atomic.AddInt64(&s.waitingTimeNS, t.waitingTimeNS)
	}()

	// 等待被调度
	t.waitAndBlock(nil)

	//
	f(t.newContext(ctx))
}

// RunningTime returns the total running time (not waiting in Yield)
// for all goroutines.
func (s *Scheduler) RunningTime() time.Duration {
	return time.Duration(atomic.LoadInt64(&s.runningTimeNS)) * time.Nanosecond
}

// BlockingTime returns the total blocking time (waiting in Yield) for
// all goroutines.
func (s *Scheduler) BlockingTime() time.Duration {
	return time.Duration(atomic.LoadInt64(&s.blockingTimeNS)) * time.Nanosecond
}

// WaitingTime returns the total waiting time (running the Wait
// function) for all goroutines.
func (s *Scheduler) WaitingTime() time.Duration {
	return time.Duration(atomic.LoadInt64(&s.waitingTimeNS)) * time.Nanosecond
}

// AvgLoad returns the average task queue size.
func (s *Scheduler) AvgLoad() float32 {
	return float32(s.sumQueued) / float32(s.numGetCalls)
}

// runQueue reads from the task queue and unblocks goroutines in Yield.
func (s *Scheduler) runQueue(q taskQueue) {
	defer s.wg.Done()

	for {
		// 将调用 `yield` 的协程放入 q 中
		if !s.recvYielded(q) {
			break
		}

		//
		s.resumeFill(q)
	}
}

// recvYielded blocks until a task has yielded or terminated. It
// receives as many tasks as are available, to maximize queue load.
func (s *Scheduler) recvYielded(q taskQueue) bool {
	// 阻塞式等待 yield 管道，获取调用 `yield` 的协程
	t, ok := <-s.yieldCh
	if !ok {
		return false
	}
	// 将协程放入优先级队列，尽可能取更多的协程，让 q 更充足
	for {
		if t != nil {
			q.Put(t)
		}
		select {
		case t, ok = <-s.yieldCh:
			if !ok {
				return false
			}
		default:
			return true
		}
	}
}

// resumeFill resumes tasks from the task queue until numP tasks are
// running, or the queue is empty.
//
// resumeFill 从任务队列中恢复任务，直到 numP 个任务正在运行，或者队列为空。
func (s *Scheduler) resumeFill(q taskQueue) {
	// 队列非空，就逐个唤醒，直到达到最大并发度
	for q.Len() > 0 {

		// 当前并发度
		n := atomic.LoadUintptr(&s.numRunning)

		// 超过限制，返回
		if n >= s.conc {
			return
		// 更新并发度(原子)
		} else if !atomic.CompareAndSwapUintptr(&s.numRunning, n, n+1) {
			continue
		}

		// 取出 t
		t := q.Get()
		s.sumQueued += q.Len() + 1
		s.numGetCalls++

		// 唤醒 t
		select {
		case t.wakeCh <- struct{}{}:
			// Continue.
		default:
			// Continue.
		}

	}
}

// runTimeSlot updates the `timeSlot` so Yield preempts a goroutine
// only slated for an earlier time slot.
func (s *Scheduler) runTimeSlot() {
	defer s.wg.Done()

	t := time.NewTicker(10 * time.Millisecond)
	defer t.Stop()

	// 每 10ms 增加 1 个 slot
	for {
		select {
		case <-t.C:
			// Continue.
		case <-s.doneCh:
			return
		}
		atomic.AddUintptr(&s.timeSlot, 1)
	}
}

// Yield blocks the goroutine if it has been preempted and waits for
// the scheduler to resume it.
//
// 如果 goroutine 被抢占了，那么 Yield 会阻止它，并等待调度器恢复它。
func Yield(ctx context.Context) {
	// 取当前 task
	t := taskFromContext(ctx)
	if t == nil {
		panic(errors.New("the context doesn't reference a Scheduler"))
	}

	// ???
	if t.timeSlot >= atomic.LoadUintptr(&t.s.timeSlot) {
		return
	}

	select {
	case <-t.s.doneCh:
		panic(errors.New("Yield was called after the scheduler was closed."))
	default:
	}

	//
	t.waitAndBlock(nil)
}

// Wait blocks the goroutine and runs `f`, accounting it as I/O wait
// time, rather than running time.
func Wait(ctx context.Context, f func()) {
	t := taskFromContext(ctx)
	if t == nil {
		panic(errors.New("the context doesn't reference a Scheduler"))
	}

	select {
	case <-t.s.doneCh:
		panic(errors.New("Wait was called after the scheduler was closed."))
	default:
	}

	t.waitAndBlock(f)
}

type task struct {
	s *Scheduler

	wakeCh   chan struct{}
	timeSlot uintptr // Zero indicates a previously unscheduled task.

	startNS        int64
	runningTimeNS  int64
	blockingTimeNS int64
	waitingTimeNS  int64

	priority int64
}

func taskFromContext(ctx context.Context) *task {
	return ctx.Value(taskKey).(*task)
}

var taskKey = new(int)

// newContext creates a context with the task embedded.
//
// 将 t 保存到 ctx 中
func (t *task) newContext(ctx context.Context) context.Context {
	return context.WithValue(ctx, taskKey, t)
}

// waitAndBlock unconditionally marks the task as blocked and sends it
// to the scheduler. If `f` is non-nil, it runs that function,
// accounted as waiting time, before blocking.
//
//
func (t *task) waitAndBlock(f func()) {
	// 从 Do/Yield/Wait 到 waitAndBlock 的执行耗时，计作 running time
	now := nowNano()
	t.runningTimeNS += now - t.startNS
	t.startNS = now

	// 运行数 -1
	atomic.AddUintptr(&t.s.numRunning, ^uintptr(0))

	// 执行 `f` 的时间计作 waiting time
	if f != nil {
		f()
		now := nowNano()
		t.waitingTimeNS += now - t.startNS
		t.startNS = now
	}

	/// >>>>>>>>>>>>>>> 下面这段耗时，计作 blocking time

	// 计算 t 的优先级
	t.priority = t.s.algo(t)

	// 发给 scheduler
	t.s.yieldCh <- t

	// 等待被调度
	<-t.wakeCh

	//
	t.timeSlot = atomic.LoadUintptr(&t.s.timeSlot)

	now = nowNano()
	t.blockingTimeNS += now - t.startNS
	t.startNS = now

	/// <<<<<<<<<<<<<<<<
}

func nowNano() int64 {
	return time.Now().UnixNano()
}
