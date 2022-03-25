package main

import (
	"errors"
	"fmt"
	"sync"
)

// Fix sized thread pool
type ThreadPool struct {
	corePoolSize 		int
	maxPoolSize 		int
	keepAliveTimeMs int
	taskQueue 			[]Task
	lock 						sync.Mutex
	taskQueueEmpty	*sync.Cond
	shutdown 				chan struct{}
	aliveThreads		chan struct{}
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

func NewThreadPool(corePoolSize int, maxPoolSize int, keepAliveTimeMs int) *ThreadPool {
	threadPool := new(ThreadPool)
	threadPool.corePoolSize = corePoolSize
	threadPool.maxPoolSize = corePoolSize
	threadPool.keepAliveTimeMs = keepAliveTimeMs
	threadPool.taskQueue = make([]Task, 0)
	threadPool.taskQueueEmpty = sync.NewCond(&threadPool.lock)
	threadPool.shutdown = make(chan struct{})
	threadPool.aliveThreads = make(chan struct{})

	// initialize worker threads
	for i := 1; i <= corePoolSize; i++ {
		go func() {
			for {
				task := threadPool.dequeue()
				result := task.executable()
				if result == nil {
					// has shutdown, exit
					threadPool.aliveThreads <- struct{}{}
					break
				}
				task.future.Put(result)
			}
		}()
	}

	return threadPool
}

func (t *ThreadPool) dequeue() *Task {
	t.lock.Lock()
	defer t.lock.Unlock()
	for len(t.taskQueue) == 0 && !t.is_shutdown() {
		t.taskQueueEmpty.Wait()
	}
	if len(t.taskQueue) == 0 {
		return nil
	}
	task := t.taskQueue[0]
	t.taskQueue = t.taskQueue[1:]
	return &task
}

func (t *ThreadPool) is_shutdown() bool {
	select {
	case <-t.shutdown:
		return true
	default:
		return false
	}
}

func (t *ThreadPool) submit(f func()(interface{})) (*Future, error) {
	select {
	case <-t.shutdown:
		return nil, errors.New("threadpool has been shut down")
	default:
	}
	t.lock.Lock()
	defer t.lock.Unlock()
	future := new(Future)
	task := Task{executable:f, future:future}
	t.taskQueue = append(t.taskQueue, task)
	t.taskQueueEmpty.Signal()
	return future, nil
}

func (t *ThreadPool) shutdownPool() {
	close(t.shutdown)
	for i := 0; i < t.corePoolSize; i++ {
		<- t.aliveThreads
	}
}

func main() {
	pool := NewThreadPool(10, 10, 1000)
	for i := 1; i <= 10; i++ {
		i := i
		pool.submit(func()(interface{}){ 
			fmt.Printf("printing %d\n", i) 
			return nil
		})
	}
	pool.shutdownPool()
}

