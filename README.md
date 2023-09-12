# gum

Go Unit Manager is a simple Goroutine unit manager for GoLang.


Features:

- Scheduling of multiple goroutines.
- Shutdown on `os.Signal` events.
- Gracefull shutdown of units


## Overview

A unit is a type that implements `WorkUnit` interface. The `Run()` method
of each registered unit is spawned in its own goroutine.

The `Manager` handles communication and synchronized shutdown procedure.


## Usage

1. Create a unit manager
2. Implement the `WorkUnit` on your goroutines
3. Add units to the manager
4. Run the manager and wait on its `Quit` channel

```golang
import (
    "os"
    "log"
    "time"
    gum "git.sp4ke.com/sp4ke/gum.git"
)

type Worker struct{}

// Example loop, will be spwaned inside a goroutine
func (w *Worker) Run(um UnitManager) {
    ticker := time.NewTicker(time.Second)

    // Worker's loop
    for {
        select {
        case <-ticker.C:
            log.Println("tick")

        // Read from channel if this worker unit should stop
        case <-um.ShouldStop():

            // Shutdown work for current unit
            w.Shutdown()

            // Notify manager that this unit is done.
            um.Done()
        }
    }
}

func (w *Worker) Shutdown() {
    // shutdown procedure for worker
    return
}

func NewWorker() *Worker {
    return &Worker{}
}

func main() {
    // Create a unit manager
    manager := gum.NewManager()

    // Shutdown all units on SIGINT
    manager.ShutdownOn(os.Interrupt)

    // NewWorker returns a type implementing WorkUnit interface unit :=
    worker := NewWorker()
    worker2 := NewWorker()

    // Register the unit with the manager
    manager.AddUnit(worker, "work1")
    manager.AddUnit(worker2, "work2")

    // Run the manager
    go manager.Run()


    // Wait for all units to shutdown gracefully through their `Shutdown` method
    <-manager.Quit
}
```

## Issues and Comments
This repo is a mirror. For any question or issues use the repo hosted at
[https://git.sp4ke.com/sp4ke/gum.git](https://git.sp4ke.com/sp4ke/gum.git)


## TODO:
- multi platform signal handling
 - syscall signals on linux
 - os.Signal on windows

