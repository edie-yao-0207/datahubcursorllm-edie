/*genuuid is a lambda function that generates a uuid4

output:
{
	"uuid": string // new uuid4
}
*/

package main

import (
	"github.com/google/uuid"

	"samsaradev.io/infra/dataplatform/lambdafunctions/util/middleware"
)

type GenUUIDOutput struct {
	UUID string `json:"uuid"`
}

func HandleRequest() (GenUUIDOutput, error) {
	return GenUUIDOutput{
		UUID: uuid.New().String(),
	}, nil
}

func main() {
	middleware.StartWrapped(HandleRequest, middleware.LogInputOutput)
}
