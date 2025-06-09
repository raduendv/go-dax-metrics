package main

import "flag"

type flags = struct {
	test string
	op   string
}

func getFlags() *flags {
	f := &flags{}

	flag.StringVar(&f.test, "test", "", "")
	flag.StringVar(&f.op, "op", "", "")
	flag.Parse()

	return f
}
