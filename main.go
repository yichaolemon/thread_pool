package main

import (
	"errors"
	"fmt"
	"math"
	"sync"
	"time"
)

type ThreadPool struct {
	corePoolSize 		int
	maxPoolSize 		int
	keepAliveTime time.Duration
	taskQueue 			[]Task
	lock 						sync.Mutex
	taskQueueEmpty	*sync.Cond
	taskQueueFull 	*sync.Cond
	shutdown 				chan struct{}
	drainedThreads	chan struct{} // used for draining threads after executor shutdown
	numAliveThreads int
	taskQueueMaxSize int
}

type Task struct {
	executable func()(interface{})
	future *Future
}

type Future struct {
	result interface{}
	done chan struct{}
}

func (f *Future) Result() interface{} {
	<-f.done
	return f.result
}

func (f *Future) Put(result interface{}) {
	f.result = result
	close(f.done)
}

func NewDefaultThreadPool(corePoolSize int, maxPoolSize int) *ThreadPool {
	return NewThreadPool(corePoolSize, maxPoolSize, math.MaxInt16, math.MaxInt16)
}

func NewThreadPool(corePoolSize int, maxPoolSize int, keepAliveTimeMs int, 
	taskQueueMaxSize int) *ThreadPool {
	threadPool := new(ThreadPool)
	threadPool.corePoolSize = corePoolSize
	threadPool.maxPoolSize = maxPoolSize
	threadPool.keepAliveTime = time.Millisecond * time.Duration(keepAliveTimeMs)
	threadPool.taskQueue = make([]Task, 0)
	threadPool.taskQueueEmpty = sync.NewCond(&threadPool.lock)
	threadPool.taskQueueFull = sync.NewCond(&threadPool.lock)
	threadPool.shutdown = make(chan struct{})
	threadPool.drainedThreads = make(chan struct{})
	threadPool.taskQueueMaxSize = taskQueueMaxSize

	// periodically calls Braodcast to wake up threads waiting to dequeue from task queue
	ticker := time.NewTicker(10 * time.Millisecond)
	go func() {
		for {
			select {
			case <-ticker.C:
				threadPool.taskQueueEmpty.Broadcast()
			case <-threadPool.shutdown:
				return
			}
		}
	}()

	// initialize worker threads
	for i := 1; i <= corePoolSize; i++ {
		threadPool.createNewThread()
	}

	return threadPool
}

func (t *ThreadPool) createNewThread() {
	go func() {
		for {
			task := t.dequeue()
			if task == nil {
				if t.isShutdown() {
					// has shutdown, drain thread
					t.drainedThreads <- struct{}{}
					t.lock.Lock()
					t.numAliveThreads -= 1
					t.lock.Unlock()
					break
				} else {
					// timed out, check if can kill this thread
					t.lock.Lock()
					if (t.numAliveThreads > t.corePoolSize) {
						fmt.Printf("thread timed out\n")
						t.numAliveThreads -= 1
						t.lock.Unlock()
						break
					}
					t.lock.Unlock()
				}
			} else {
				result := task.executable()
				task.future.Put(result)
			}
		}
	}()
	t.lock.Lock()
	defer t.lock.Unlock()
	t.numAliveThreads += 1
	fmt.Printf("new-thread-{%d} added\n", t.numAliveThreads)
}

func (t *ThreadPool) dequeue() *Task {
	t.lock.Lock()
	defer t.lock.Unlock()

	waitStartTime := time.Now()
	for len(t.taskQueue) == 0 && !t.isShutdown() && 
	!(time.Since(waitStartTime) > t.keepAliveTime && t.numAliveThreads > t.corePoolSize) {
		t.taskQueueEmpty.Wait()
	}
	if len(t.taskQueue) == 0 {
		return nil
	}
	task := t.taskQueue[0]
	t.taskQueue = t.taskQueue[1:]
	t.taskQueueFull.Signal()
	return &task
}

func (t *ThreadPool) isShutdown() bool {
	select {
	case <-t.shutdown:
		return true
	default:
		return false
	}
}

func (t *ThreadPool) shouldCreateNewThread() bool {
	t.lock.Lock()
	defer t.lock.Unlock()
	return t.numAliveThreads < t.maxPoolSize && len(t.taskQueue) == t.taskQueueMaxSize
}

func (t *ThreadPool) submit(f func()(interface{})) (*Future, error) {
	select {
	case <-t.shutdown:
		return nil, errors.New("threadpool has been shut down")
	default:
	}
	future := &Future{
		done: make(chan struct{}),
	}
	task := Task{executable:f, future:future}

	// potentially add a new thread to the pool
	if t.shouldCreateNewThread() {
		t.createNewThread()
	}

	t.lock.Lock()
	defer t.lock.Unlock()
	// block
	for len(t.taskQueue) >= t.taskQueueMaxSize {
		t.taskQueueFull.Wait()
	}

	t.taskQueue = append(t.taskQueue, task)
	t.taskQueueEmpty.Signal()
	return future, nil
}

func (t *ThreadPool) shutdownPool() {
	t.lock.Lock()
	close(t.shutdown)
	numThreadsToDrain := t.numAliveThreads
	t.lock.Unlock()
	// needs to wake up threads waiting on deque
	t.taskQueueEmpty.Broadcast()
	for i := 1; i <= numThreadsToDrain; i++ {
		fmt.Printf("draining %d/%d threads\n", i, numThreadsToDrain)
		<- t.drainedThreads
	}
}

func main() {
	pool := NewThreadPool(15, 30, 50, 50)
	for i := 1; i <= 1000; i++ {
		i := i
		pool.submit(func()(interface{}){ 
			fmt.Printf("printing %d\n", i) 
			return nil
		})
	}
	time.Sleep(time.Second * 3)
	pool.shutdownPool()
}

