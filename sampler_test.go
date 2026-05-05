// sampler_test.go
package main

import "testing"

func TestSanity(t *testing.T) {
	if 1+1 != 2 {
		t.Fatal("math broken")
	}
}
