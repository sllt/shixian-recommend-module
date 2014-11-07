package main

import (
	"flag"
	"fmt"
	"os"
	"strings"

	"doraemon/model"
	"doraemon/modual/config"
	"doraemon/task"
)

func checkErr(err error) {
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
}

func Usage() {
	fmt.Fprint(os.Stderr, "Usage of ", os.Args[0], ":\n")
	flag.PrintDefaults()
	fmt.Fprint(os.Stderr, "\n")
	os.Exit(1)
}

type Task interface {
	DoDataTask(inputFiles []string, outputFile string, arg interface{}) error
}

func DoDataJob(task Task, inputFiles []string, outputFile string, arg interface{}) error {
	return task.DoDataTask(inputFiles, outputFile, arg)
}

func main() {
	flag.Usage = Usage
	input := flag.String("i", "", "Input file")
	output := flag.String("o", "", "Output file")
	conf := flag.String("c", "", "Conf File")

	var serviceTypeSupport string
	for k, _ := range task.Adapters {
		serviceTypeSupport += k + "|"
	}
	serviceTypeSupport = strings.TrimRight(serviceTypeSupport, "|")

	serviceType := flag.String("s", "", "Service type["+serviceTypeSupport+"]")
	serviceArg := flag.String("a", "", "Service Argument")

	flag.Parse()

	if *input == "" {
		Usage()
	}

	if *output == "" {
		Usage()
	}

	if *conf == "" {
		Usage()
	}

	var err error
	err = config.NewConfigFile("json", *conf, model.GlobalConf)
	checkErr(err)

	inputFiles := strings.Split(strings.TrimSpace(*input), ",")
	outputFile := *output

	var dataTask Task
	dataTask, err = task.NewDataTask(*serviceType)
	checkErr(err)

	err = DoDataJob(dataTask, inputFiles, outputFile, *serviceArg)
	checkErr(err)
}
