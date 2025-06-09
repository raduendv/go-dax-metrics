package main

import (
	"flag"
	"fmt"
	"go-dax-metrics/pkg/helper"
)

func printMissingTest(name string, appConfig helper.AppConfig) {
	fmt.Printf("Test name unknown: \"%s\"\nAvailable tests:\n", name)
	var n string
	for n, _ = range appConfig.TestConfigs {
		fmt.Printf("\t%s\n", n)
	}

	fmt.Printf("Example: Run the application with -test=%s -op=...\n", n)
}

func printMissingTrafficConfig(test, name string, appConfig helper.AppConfig) {
	fmt.Printf("Test %s has property TrafficConfig missconfigured: \"%s\".\nAvailable TrafficConfigs:\n", test, name)
	var n string
	for n, _ = range appConfig.TrafficConfigs {
		fmt.Printf("\t%s\n", n)
	}
}

func printMissingItemSize(test, name string, sizes map[string]int64) {
	fmt.Printf("Unknown operation: \"%s\"\nAvailable operations:\n", name)
	var n string
	for n, _ = range sizes {
		fmt.Printf("\t%s\n", n)
	}

	fmt.Printf("Example: Run the application with -test=%s -op=%s\n", test, n)

}

func main() {
	appConfig := helper.GetAppConfig()

	testFlag := flag.String("test", "", "Test to run")
	operationFlag := flag.String("op", "", "Operation to run")
	flag.Parse()

	testConfig, ok := appConfig.TestConfigs[*testFlag]
	if !ok {
		printMissingTest(*testFlag, appConfig)

		return
	}

	traficConfig, ok := appConfig.TrafficConfigs[testConfig.TrafficConfig]
	if !ok {
		printMissingTrafficConfig(*testFlag, testConfig.TrafficConfig, appConfig)

		return
	}

	clientConfig, ok := appConfig.ClientConfigs[testConfig.ClientConfig]
	if !ok {
		printMissingTrafficConfig(*testFlag, testConfig.TrafficConfig, appConfig)

		return
	}

	size, ok := traficConfig.ItemSizes[*operationFlag]
	if !ok {
		printMissingItemSize(*testFlag, *operationFlag, traficConfig.ItemSizes)

		return
	}

	_ = size

	m := helper.NewManager(
		testConfig.Endpoint,
		appConfig.Table,
		testConfig,
		clientConfig,
		traficConfig,
		*operationFlag,
	)

	m.Run()
}
