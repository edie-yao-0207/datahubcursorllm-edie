package ksdeltalake

import (
	"reflect"
	"sort"
)

// RegistryStat maintains a sorted collection of Stats
type RegistryStat struct {
	stats []*Stat
}

// NewRegistryStat creates a new RegistryStat and automatically discovers and registers all Stat-returning functions
func NewRegistryStat() *RegistryStat {
	registry := &RegistryStat{
		stats: make([]*Stat, 0),
	}

	// Initialize registry with discovered stats
	registry.discoverAndRegisterStats()

	// Sort once after all stats are discovered
	sort.Slice(registry.stats, func(i, j int) bool {
		if registry.stats[i].Kind != registry.stats[j].Kind {
			return registry.stats[i].Kind < registry.stats[j].Kind
		}
		return registry.stats[i].StatType < registry.stats[j].StatType
	})

	return registry
}

// GetStats returns all registered Stats
func (r *RegistryStat) GetStatsAll() []*Stat {
	return r.stats
}

// discoverAndRegisterStats automatically discovers and registers all Stat-returning functions
func (r *RegistryStat) discoverAndRegisterStats() {
	registryType := reflect.TypeOf(r)

	for i := 0; i < registryType.NumMethod(); i++ {
		method := registryType.Method(i)
		methodType := method.Type

		// Skip methods with parameters (other than receiver)
		if methodType.NumIn() > 1 {
			continue
		}

		// Skip methods that don't return exactly one value
		if methodType.NumOut() != 1 {
			continue
		}

		returnType := methodType.Out(0)

		// Check if the function returns a *Stat (not []*Stat like GetStats)
		if returnType.String() == "*ksdeltalake.Stat" {
			result := method.Func.Call([]reflect.Value{reflect.ValueOf(r)})
			if stat, ok := result[0].Interface().(*Stat); ok {
				r.stats = append(r.stats, stat)
			}
		}
	}
}
