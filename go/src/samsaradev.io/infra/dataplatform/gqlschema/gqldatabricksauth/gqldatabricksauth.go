package gqldatabricksauth

import (
	"go.uber.org/fx"

	"samsaradev.io/fleet/dataproducts/dataproductsproto"
	"samsaradev.io/infra/config"
	"samsaradev.io/infra/fxregistry"
	"samsaradev.io/infra/thunder/graphql/schemabuilder"
	"samsaradev.io/models"
)

func init() {
	fxregistry.MustRegisterDefaultConstructor(NewServer)
}

type Server struct {
	Config                  *config.AppConfig
	BenchmarksServiceClient dataproductsproto.BenchmarksServiceClient
}

type Params struct {
	fx.In
	Config                  *config.AppConfig
	BenchmarksServiceClient dataproductsproto.BenchmarksServiceClient
}

func NewServer(params Params) *Server {
	return &Server{
		Config:                  params.Config,
		BenchmarksServiceClient: params.BenchmarksServiceClient,
	}
}

func (s *Server) Register(schema *schemabuilder.Schema) {
	s.registerUser(schema)
}

func (s *Server) registerUser(schema *schemabuilder.Schema) {
	object := schema.Object("User", models.User{})
	object.BatchFieldFunc("databricksUserAuthStatus", s.getDatabricksUserAuthStatusBatch, schemabuilder.Expensive)
}
