# Submit Job Lambda

Notes on running this lambda end-to-end locally.
1. You'll want to comment out the `middleware.StartWrapped(...)` line in the lambda handler, and directly call
   the main lambda handler logic from the `main` method. Please see `main_test.go` for an example
   of realistic inputs you can use.
2. From `go/src/samsaradev.io` you'll want to run
   `$ AWS_DEFAULT_PROFILE=databricks-superadmin go run infra/dataplatform/lambdafunctions/datapipelines/submit_job/main.go`
		The databricks-superadmin profile is required to read from SSM to get the host/token for databricks.
3. If the job succeeds, you should receive a run id back which you can then get more info about
   using the databricks API.
   (Auth is done by Bearer token using the token fetched from ssm)
   https://docs.databricks.com/dev-tools/api/latest/jobs.html#runs-get
