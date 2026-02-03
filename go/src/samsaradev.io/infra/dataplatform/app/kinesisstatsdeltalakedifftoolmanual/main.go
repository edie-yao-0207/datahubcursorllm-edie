package main

import "samsaradev.io/infra/dataplatform/app/kinesisstatsdeltalakedifftool"

// App for the manual version of the diff tool, that runs without leader election so that multiple runs can be done
// concurrently. These runs are invoked via manual ECS calls.
func main() {
	kinesisstatsdeltalakedifftool.DiffToolAppMain(false)
}
