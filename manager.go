package gum

import (
	"log"
	"os"
	"os/signal"
	"reflect"
	"strconv"
	"strings"
)

var idGenerator = genId()

// The WorkUnit interface is used to define a unit of work.
// The Run method will be called in a goroutine.
type WorkUnit interface {
	Run(UnitManager)
}

// The UnitManager interface is used to manage a unit of work.
// The ShouldStop method returns a channel that will be closed when the unit
// should stop.
// The Done method should be called when the unit is done.
type UnitManager interface {
	ShouldStop() <-chan bool
	Done()
	Panic(err error)
}

type WorkUnitManager struct {
	stop       chan bool
	workerQuit chan bool
	unit       WorkUnit
	panic      chan error
	isPaniced  bool
}

func (w *WorkUnitManager) ShouldStop() <-chan bool {
	return w.stop
}

func (w *WorkUnitManager) Done() {
	w.workerQuit <- true
}

func (w *WorkUnitManager) Panic(err error) {
	w.panic <- err
	w.isPaniced = true
	w.workerQuit <- true
	close(w.stop)
}

type Manager struct {
	signalIn chan os.Signal

	shutdownSigs []os.Signal

	workers map[string]*WorkUnitManager

	Quit chan bool

	panic chan error // Used for panicing goroutines
}

func (m *Manager) Run() {
	log.Println("Starting manager ...")

	for unitName, w := range m.workers {
		log.Printf("Starting <%s>\n", unitName)
		go w.unit.Run(w)
	}

	for {
		select {
		case sig := <-m.signalIn:

			if !in(m.shutdownSigs, sig) {
				break
			}

			log.Println("shutting event received ... ")

			// send shutdown event to all worker units
			for name, w := range m.workers {
				log.Printf("shutting down <%s>\n", name)
				w.stop <- true
			}

			// Wait for all units to quit
			for name, w := range m.workers {
				<-w.workerQuit
				log.Printf("<%s> down", name)
			}

			// All workers have shutdown
			log.Println("All workers have shutdown, shutting down manager ...")

			m.Quit <- true

		case p := <-m.panic:

			for name, w := range m.workers {
				if w.isPaniced {
					log.Printf("Panicing for <%s>: %s", name, p)
				}
			}

			for name, w := range m.workers {
				log.Printf("shuting down <%s>\n", name)
				if !w.isPaniced {
					w.stop <- true
				}
			}

			// Wait for all units to quit
			for name, w := range m.workers {
				<-w.workerQuit
				log.Printf("<%s> down", name)
			}

			// All workers have shutdown
			log.Println("All workers have shutdown, shutting down manager ...")

			m.Quit <- true

		}
	}
}

func (m *Manager) ShutdownOn(sig ...os.Signal) {

	for _, s := range sig {
		log.Printf("Registering shutdown signal: %s\n", s)
		signal.Notify(m.signalIn, s)
	}

	m.shutdownSigs = append(m.shutdownSigs, sig...)
}

type IDGenerator func(string) int

func genId() IDGenerator {
	ids := make(map[string]int)

	return func(unit string) int {
		ret := ids[unit]
		ids[unit]++
		return ret
	}
}

func (m *Manager) AddUnit(unit WorkUnit) {

	workUnitManager := &WorkUnitManager{
		workerQuit: make(chan bool, 1),
		stop:       make(chan bool, 1),
		unit:       unit,
		panic:      m.panic,
	}

	unitType := reflect.TypeOf(unit)
	unitName := strings.Split(unitType.String(), ".")[1]

	unitId := idGenerator(unitName)
	unitName += strconv.Itoa(unitId)

	log.Println("Adding unit ", unitName)

	m.workers[unitName] = workUnitManager
}

func NewManager() *Manager {
	return &Manager{
		signalIn: make(chan os.Signal, 1),
		Quit:     make(chan bool, 1),
		workers:  make(map[string]*WorkUnitManager),
		panic:    make(chan error, 1),
	}
}

// Test if signal is in array
func in(arr []os.Signal, sig os.Signal) bool {
	for _, s := range arr {
		if s == sig {
			return true
		}
	}
	return false
}
