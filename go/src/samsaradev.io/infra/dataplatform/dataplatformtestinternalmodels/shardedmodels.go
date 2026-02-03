package dataplatformtestinternalmodels

import (
	"context"
	"fmt"

	"github.com/samsarahq/go/oops"
	"go.uber.org/fx"

	"samsaradev.io/infra/thunder/sqlgen"

	"samsaradev.io/helpers/monitoring"
	"samsaradev.io/helpers/shardhelpers"
	"samsaradev.io/infra/config"
	"samsaradev.io/infra/dbtools/dbconfighelpers"
	"samsaradev.io/infra/dbtools/dbhelpers"
	"samsaradev.io/infra/fxregistry"
	"samsaradev.io/infra/monitoring/monitoringhelpers/apptags"
	"samsaradev.io/shards"
)

func init() {
	fxregistry.MustRegisterDefaultConstructor(NewPrimaryShardedDataplatformtestinternalModels)
	fxregistry.MustRegisterDefaultConstructor(NewReplicaShardedDataplatformtestinternalModels)
}

type ShardedDataplatformtestinternalModels struct {
	shardToModels map[string]*DataplatformtestinternalModels
	shardMapper   shards.ShardProvider
	isPrimary     bool
	shards        []string
}

type PrimaryShardedDataplatformtestinternalModelsInput struct {
	fx.In
	ShardedDataplatformtestinternalModels *ShardedDataplatformtestinternalModels `name:"rw"`
}

type PrimaryShardedDataplatformtestinternalModelsOutput struct {
	fx.Out
	ShardedDataplatformtestinternalModels *ShardedDataplatformtestinternalModels `name:"rw"`
}

type ReplicaShardedDataplatformtestinternalModelsInput struct {
	fx.In
	ShardedDataplatformtestinternalModels *ShardedDataplatformtestinternalModels `name:"ro"`
}

type ReplicaShardedDataplatformtestinternalModelsOutput struct {
	fx.Out
	ShardedDataplatformtestinternalModels *ShardedDataplatformtestinternalModels `name:"ro"`
}

type ShardedDataplatformtestinternalModelsParams struct {
	fx.In
	Config             *config.AppConfig
	Lifecycle          fx.Lifecycle
	ShardMapperFactory shards.ShardProviderFactory
}

func newPrimaryDataplatformtestinternalModelsFromShard(p ShardedDataplatformtestinternalModelsParams, shardName string) (*DataplatformtestinternalModels, error) {
	dbConfigForShard := dbconfighelpers.GetShardDataplatformtestinternalConfigForCurrentClusterAndApp(p.Config, shardName)
	DataplatformtestinternalModels, err := newDataplatformtestinternalModels(p.Config, dbConfigForShard, p.Lifecycle)

	if p.Config.IsTest {
		dbhelpers.ResetDatabaseForTest(DataplatformtestinternalModels.DB())
	}
	return DataplatformtestinternalModels, oops.Wrapf(err, "create new Dataplatformtestinternal models")
}

func newReplicaDataplatformtestinternalModelsFromShard(p ShardedDataplatformtestinternalModelsParams, shardName string) (*DataplatformtestinternalModels, error) {
	dbConfigForShard := dbconfighelpers.GetShardDataplatformtestinternalReplicaConfigForCurrentClusterAndApp(p.Config, shardName)
	DataplatformtestinternalModels, err := newDataplatformtestinternalModels(p.Config, dbConfigForShard, p.Lifecycle)
	return DataplatformtestinternalModels, oops.Wrapf(err, "create new Dataplatformtestinternal models")
}

func NewPrimaryShardedDataplatformtestinternalModels(p ShardedDataplatformtestinternalModelsParams) (PrimaryShardedDataplatformtestinternalModelsOutput, error) {
	shardedDataplatformtestinternalConfigs := dbconfighelpers.GetShardedDataplatformtestinternalConfigForCurrentClusterAndApp(p.Config)
	shardToModels := make(map[string]*DataplatformtestinternalModels, len(shardedDataplatformtestinternalConfigs.Shards))
	var shardNames []string
	for shardName := range shardedDataplatformtestinternalConfigs.Shards {
		shardNames = append(shardNames, shardName)
		shard, err := newPrimaryDataplatformtestinternalModelsFromShard(p, shardName)
		// When adding new shards, configs propagate before the database is ready to connect.
		// We're going to skip adding database shards when we fail to connect so that services
		// do not panic on startup.
		if err != nil {
			tags := []string{
				fmt.Sprintf("shardname:%s", shardName),
				"type:dataplatformtestinternal",
				"isreader:false",
			}
			tags = append(tags, apptags.GetAppTags()...)
			monitoring.AggregatedDatadog.Incr("db.shard.connect.failure", tags...)
			continue
		}
		shardToModels[shardName] = shard
	}

	shardMapper := p.ShardMapperFactory.NewShardMapper(shardhelpers.DatabaseShard, shardhelpers.DataplatformtestinternalDataType)

	shardedDataplatformtestinternalModels := &ShardedDataplatformtestinternalModels{
		shardToModels: shardToModels,
		shardMapper:   shardMapper,
		shards:        shardNames,
		isPrimary:     true,
	}

	return PrimaryShardedDataplatformtestinternalModelsOutput{
		ShardedDataplatformtestinternalModels: shardedDataplatformtestinternalModels,
	}, nil
}

func NewReplicaShardedDataplatformtestinternalModels(p ShardedDataplatformtestinternalModelsParams) (ReplicaShardedDataplatformtestinternalModelsOutput, error) {
	shardedDataplatformtestinternalConfigs := dbconfighelpers.GetShardedDataplatformtestinternalConfigForCurrentClusterAndApp(p.Config)
	shardToModels := make(map[string]*DataplatformtestinternalModels, len(shardedDataplatformtestinternalConfigs.Shards))
	var shardNames []string
	for shardName := range shardedDataplatformtestinternalConfigs.Shards {
		shardNames = append(shardNames, shardName)
		shard, err := newReplicaDataplatformtestinternalModelsFromShard(p, shardName)
		// When adding new shards, configs propagate before the database is ready to connect.
		// We're going to skip adding database shards when we fail to connect so that services
		// do not panic on startup.
		if err != nil {
			tags := []string{
				fmt.Sprintf("shardname:%s", shardName),
				"type:dataplatformtestinternal",
				"isreader:false",
			}
			tags = append(tags, apptags.GetAppTags()...)
			monitoring.AggregatedDatadog.Incr("db.shard.connect.failure", tags...)
			continue
		}
		shardToModels[shardName] = shard
	}

	shardMapper := p.ShardMapperFactory.NewShardMapper(shardhelpers.DatabaseShard, shardhelpers.DataplatformtestinternalDataType)

	shardedDataplatformtestinternalModels := &ShardedDataplatformtestinternalModels{
		shardToModels: shardToModels,
		shardMapper:   shardMapper,
		shards:        shardNames,
	}

	return ReplicaShardedDataplatformtestinternalModelsOutput{
		ShardedDataplatformtestinternalModels: shardedDataplatformtestinternalModels,
	}, nil
}

// ForShard returns the DataplatformtestinternalModels for the given shard.
func (s *ShardedDataplatformtestinternalModels) ForShard(ctx context.Context, shard string) (*DataplatformtestinternalModels, error) {
	models, ok := s.shardToModels[shard]
	if !ok {
		return nil, oops.Errorf("no such shard: %s", shard)
	}
	return models, nil
}

// ForOrg returns the DataplatformtestinternalModels for the specified org.
func (s *ShardedDataplatformtestinternalModels) ForOrg(ctx context.Context, orgId int64) (*DataplatformtestinternalModels, error) {
	shardInfo, err := s.shardMapper.Shard(ctx, orgId, shardhelpers.DatabaseShard, shardhelpers.DataplatformtestinternalDataType)
	if err != nil {
		return nil, oops.Wrapf(err, "shard lookup for org: %d", orgId)
	}
	if shardInfo.ReadOnly && s.isPrimary {
		return nil, oops.Errorf("org: %d is currenty in read only mode", orgId)
	}
	models, ok := s.shardToModels[shardInfo.ShardName]
	if !ok {
		return nil, oops.Errorf("no such shard: %s", shardInfo.ShardName)
	}
	shardedSqlDB, err := models.sqlgenDB.WithShardLimit(sqlgen.Filter{"org_id": orgId})
	if err != nil {
		return nil, oops.Wrapf(err, "for org")
	}
	modelsCopy := *models
	modelsCopy.sqlgenDB = shardedSqlDB
	bridgesCopy := *models.Bridges
	modelsCopy.Bridges = &bridgesCopy

	modelsCopy.initializeBridges()

	return &modelsCopy, nil
}

// Shards returns the list of shards that can be mapped to DataplatformtestinternalModels.
func (s *ShardedDataplatformtestinternalModels) Shards() []string {
	return s.shards
}
