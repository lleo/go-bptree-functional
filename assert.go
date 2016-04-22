package bptree

import (
	"os"
	"strings"
)

//ASSERT is a global variable to be used a block calls to the assert() function.
//ASSERT is initially set by the BPTREE_DEBUG env var. If
//BPTREE_DEBUG is set to t, true, yes, or on, then ASSERT=true,
//else ASSERT=false. Capitalization in the BPTREE_DEBUG env var does not
//matter as its contents are always lower cased.
var ASSERT = strings.ToLower(os.Getenv("BPTREE_DEBUG")) == "t" ||
	strings.ToLower(os.Getenv("BPTREE_DEBUG")) == "true" ||
	strings.ToLower(os.Getenv("BPTREE_DEBUG")) == "yes" ||
	strings.ToLower(os.Getenv("BPTREE_DEBUG")) == "on"

//assert tests cond is false, then call lgr.Panic(msg).
//The best way to use this is to make it conditional with ASSERT.
//The multiline version of this conditional is:
//
//    if ASSERT {
//        assert(validateFoo(...), "Foo is invalid")
//    }
//
//The single line version of this conditional is:
//
//   _ = ASSERT && assert(validateFoo(...), "Foo is invalid")
//
func assert(cond bool, msg string) {
	if !cond {
		lgr.Panic("ASSERT: " + msg)
	}
}

//assertf tests cond is false, then call lgr.Panicf(format, args...).
//to the format as in fmt.Printf. For example:
//
//    _ = ASSERT && assertf(foo == 0, "foo != 0; foo == %d", foo)
//
func assertf(cond bool, format string, args ...interface{}) {
	if !cond {
		lgr.Panicf("ASSERT: "+format, args...)
	}
}
