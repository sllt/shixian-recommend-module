package task

import (
	"fmt"
)

// Job
type Job struct {
	data   string
	result chan<- Result
}

func (this *Job) String() string {
	if this == nil {
		return "<nil>"
	}
	return fmt.Sprintf("[Job](%+v)", *this)
}

// Result
type Result struct {
	data string
}

func (this *Result) String() string {
	if this == nil {
		return "<nil>"
	}
	return fmt.Sprintf("[Result](%+v)", *this)
}

type DataTask interface {
	DoDataTask(inputFiles []string, outputFile string, arg interface{}) error
}

var Adapters = make(map[string]DataTask)

// Register makes a DataTask adapter available by the adapter name.
// If Register is called twice with the same name or if driver is nil,
// it panics.
func Register(name string, task DataTask) {
	if task == nil {
		panic("DataTask: Register adapter is nil")
	}

	if _, dup := Adapters[name]; dup {
		panic("DataTask: Register called twice for adapter " + name)
	}

	Adapters[name] = task
}

// Create a new DataTask driver by adapter name.
func NewDataTask(adapterName string) (DataTask, error) {
	adapter, ok := Adapters[adapterName]
	if !ok {
		return nil, fmt.Errorf("DataTask: unknown adapterName %q (forgotten import?)", adapterName)
	}

	return adapter, nil
}
