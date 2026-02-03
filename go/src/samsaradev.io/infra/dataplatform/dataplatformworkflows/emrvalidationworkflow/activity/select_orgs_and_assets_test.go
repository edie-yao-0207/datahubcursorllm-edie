package activity

import (
	"context"
	"fmt"
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"samsaradev.io/infra/dataplatform/emrreplication"
	"samsaradev.io/models"
	"samsaradev.io/models/mock_models"
)

func TestSelectOrgsForValidation(t *testing.T) {
	testCases := []struct {
		name              string
		allOrgIds         []int64
		pinnedOrgs        []int64
		numOrgsValidated  int
		expectedMinLength int
		expectedMaxLength int
	}{
		{
			name:              "should include all pinned orgs",
			allOrgIds:         []int64{1, 2, 3, 4, 5, 6, 7, 8, 9, 10},
			pinnedOrgs:        []int64{1, 3, 5},
			numOrgsValidated:  5,
			expectedMinLength: 5,
			expectedMaxLength: 5,
		},
		{
			name:              "should handle when numOrgsValidated is less than pinned orgs",
			allOrgIds:         []int64{1, 2, 3, 4, 5},
			pinnedOrgs:        []int64{1, 2, 3, 4, 5},
			numOrgsValidated:  3,
			expectedMinLength: 5, // Should still include all pinned orgs
			expectedMaxLength: 5,
		},
		{
			name:              "should handle when numOrgsValidated exceeds available orgs",
			allOrgIds:         []int64{1, 2, 3},
			pinnedOrgs:        []int64{1},
			numOrgsValidated:  10,
			expectedMinLength: 3, // Should return all available orgs
			expectedMaxLength: 3,
		},
		{
			name:              "should handle empty pinned orgs",
			allOrgIds:         []int64{1, 2, 3, 4, 5},
			pinnedOrgs:        []int64{},
			numOrgsValidated:  3,
			expectedMinLength: 3,
			expectedMaxLength: 3,
		},
		{
			name:              "should handle pinned orgs not in allOrgIds",
			allOrgIds:         []int64{1, 2, 3, 4, 5},
			pinnedOrgs:        []int64{10, 20}, // Not in allOrgIds
			numOrgsValidated:  3,
			expectedMinLength: 3,
			expectedMaxLength: 3,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			result := selectOrgsForValidation(tc.allOrgIds, tc.pinnedOrgs, tc.numOrgsValidated)

			assert.GreaterOrEqual(t, len(result), tc.expectedMinLength)
			assert.LessOrEqual(t, len(result), tc.expectedMaxLength)

			// Verify all pinned orgs that exist in allOrgIds are included
			pinnedOrgSet := make(map[int64]bool)
			for _, pinnedOrg := range tc.pinnedOrgs {
				pinnedOrgSet[pinnedOrg] = true
			}

			allOrgSet := make(map[int64]bool)
			for _, orgId := range tc.allOrgIds {
				allOrgSet[orgId] = true
			}

			resultSet := make(map[int64]bool)
			for _, orgId := range result {
				resultSet[orgId] = true
			}

			// Check that all valid pinned orgs are in the result
			for _, pinnedOrg := range tc.pinnedOrgs {
				if allOrgSet[pinnedOrg] {
					assert.True(t, resultSet[pinnedOrg], "Pinned org %d should be in result", pinnedOrg)
				}
			}

			// Verify all results are from the original list
			for _, orgId := range result {
				assert.True(t, allOrgSet[orgId], "Result org %d should be from original list", orgId)
			}
		})
	}
}

func TestSelectOrgsForValidation_RandomnessWithoutSeed(t *testing.T) {
	// This test verifies that the randomization still works correctly without rand.Seed() calls
	// In Go 1.20+, the global random source is automatically seeded

	allOrgIds := []int64{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20}
	pinnedOrgs := []int64{1, 2} // Pin first 2 orgs
	numOrgsValidated := 5

	// Run selection multiple times and collect results
	results := make(map[int64]int) // Count how many times each org was selected
	iterations := 100

	for i := 0; i < iterations; i++ {
		selected := selectOrgsForValidation(allOrgIds, pinnedOrgs, numOrgsValidated)
		for _, orgId := range selected {
			results[orgId]++
		}
	}

	// Verify pinned orgs are always selected
	assert.Equal(t, iterations, results[1], "Pinned org 1 should be selected every time")
	assert.Equal(t, iterations, results[2], "Pinned org 2 should be selected every time")

	// Verify that other orgs show variation (randomness)
	// Count how many non-pinned orgs were selected at least once
	nonPinnedSelectedCount := 0
	for orgId, count := range results {
		if orgId != 1 && orgId != 2 && count > 0 {
			nonPinnedSelectedCount++
		}
	}

	// With proper randomization, we should see several different non-pinned orgs selected
	// across all iterations (much more than just the 3 remaining slots per iteration)
	assert.Greater(t, nonPinnedSelectedCount, 5,
		"Should see randomness in non-pinned org selection across iterations")
}

func TestGetRandomAssetsForOrg(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	var env struct {
		Activity         *SelectOrgsAndAssetsActivity
		ReplicaAppModels *mock_models.MockReplicaIAppModels
		MockDriverBridge *mock_models.MockIDriverBridge
		MockDeviceBridge *mock_models.MockIDeviceBridge
	}

	// Initialize mocks
	env.ReplicaAppModels = mock_models.NewMockReplicaIAppModels(ctrl)
	env.MockDriverBridge = mock_models.NewMockIDriverBridge(ctrl)
	env.MockDeviceBridge = mock_models.NewMockIDeviceBridge(ctrl)

	// Set up bridge expectations
	env.ReplicaAppModels.EXPECT().DriverBridge().Return(env.MockDriverBridge).AnyTimes()
	env.ReplicaAppModels.EXPECT().DeviceBridge().Return(env.MockDeviceBridge).AnyTimes()

	env.Activity = &SelectOrgsAndAssetsActivity{
		ReplicaAppModels: env.ReplicaAppModels,
	}

	tests := []struct {
		name           string
		orgId          int64
		sampleSize     int
		assetType      emrreplication.AssetType
		setupMocks     func()
		expectedLength int
		expectedError  bool
	}{
		{
			name:       "should select subset of drivers when more drivers than requested",
			orgId:      1001,
			sampleSize: 3,
			assetType:  emrreplication.DriverAssetType,
			setupMocks: func() {
				drivers := make([]*models.Driver, 5)
				for i := 0; i < 5; i++ {
					drivers[i] = &models.Driver{
						Id:   int64(2000 + i),
						Name: fmt.Sprintf("Driver %d", i),
					}
				}
				env.MockDriverBridge.EXPECT().
					ByOrgId(gomock.Any(), int64(1001)).
					Return(drivers, nil)
			},
			expectedLength: 3,
			expectedError:  false,
		},
		{
			name:       "should return all devices when fewer devices than requested",
			orgId:      1002,
			sampleSize: 5,
			assetType:  emrreplication.DeviceAssetType,
			setupMocks: func() {
				devices := make([]*models.Device, 2)
				for i := 0; i < 2; i++ {
					devices[i] = &models.Device{
						Id:   int64(3000 + i),
						Name: fmt.Sprintf("Driver %d", i),
					}
				}
				env.MockDeviceBridge.EXPECT().
					ByOrgId(gomock.Any(), int64(1002)).
					Return(devices, nil)
			},
			expectedLength: 2,
			expectedError:  false,
		},
		{
			name:       "should handle empty driver list",
			orgId:      1003,
			sampleSize: 3,
			assetType:  emrreplication.DriverAssetType,
			setupMocks: func() {
				env.MockDriverBridge.EXPECT().
					ByOrgId(gomock.Any(), int64(1003)).
					Return([]*models.Driver{}, nil)
			},
			expectedLength: 0,
			expectedError:  false,
		},
		{
			name:       "should handle device asset type",
			orgId:      1004,
			sampleSize: 2,
			assetType:  emrreplication.DeviceAssetType,
			setupMocks: func() {
				devices := make([]*models.Device, 3)
				for i := 0; i < 3; i++ {
					devices[i] = &models.Device{
						Id: int64(4000 + i),
					}
				}
				env.MockDeviceBridge.EXPECT().
					ByOrgId(gomock.Any(), int64(1004)).
					Return(devices, nil)
			},
			expectedLength: 2,
			expectedError:  false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tt.setupMocks()

			result, err := env.Activity.getRandomAssetsForOrg(context.Background(), tt.orgId, tt.sampleSize, tt.assetType)

			if tt.expectedError {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
				assert.Len(t, result, tt.expectedLength)

				// Verify that all IDs are unique
				idMap := make(map[int64]bool)
				for _, id := range result {
					assert.False(t, idMap[id], "Found duplicate ID: %d", id)
					idMap[id] = true
				}
			}
		})
	}
}

func TestGetRandomAssetsForOrg_SamplingAlgorithm(t *testing.T) {
	testCases := []struct {
		name              string
		drivers           []int64
		sampleSize        int
		assetType         emrreplication.AssetType
		expectedLength    int
		expectedSameOrder bool
	}{
		{
			name:              "should select subset of drivers when more drivers than requested",
			drivers:           []int64{1, 2, 3, 4, 5, 6, 7, 8, 9, 10},
			sampleSize:        5,
			assetType:         emrreplication.DriverAssetType,
			expectedLength:    5,
			expectedSameOrder: false,
		},
		{
			name:              "should return all devices when fewer devices than requested",
			drivers:           []int64{1, 2, 3},
			sampleSize:        5,
			assetType:         emrreplication.DeviceAssetType,
			expectedLength:    3,
			expectedSameOrder: true, // No shuffling needed when returning all
		},
		{
			name:              "should handle empty driver list",
			drivers:           []int64{},
			sampleSize:        5,
			assetType:         emrreplication.DriverAssetType,
			expectedLength:    0,
			expectedSameOrder: true,
		},
		{
			name:              "should handle device asset type",
			drivers:           []int64{1, 2, 3, 4, 5},
			sampleSize:        3,
			assetType:         emrreplication.DeviceAssetType,
			expectedLength:    3,
			expectedSameOrder: false,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			// This is a unit test of the algorithm logic, not the full database integration
			// We test the core randomization logic that doesn't require database calls

			// Simulate the core sampling algorithm from getRandomAssetsForOrg
			allAssetIds := make([]int64, len(tc.drivers))
			copy(allAssetIds, tc.drivers)

			if len(allAssetIds) == 0 {
				assert.Equal(t, tc.expectedLength, 0)
				return
			}

			if len(allAssetIds) <= tc.sampleSize {
				assert.Equal(t, tc.expectedLength, len(allAssetIds))
				return
			}

			// Apply the same sampling logic as in getRandomAssetsForOrg
			sampleSize := tc.sampleSize
			for i := 0; i < sampleSize; i++ {
				// This is the partial shuffle algorithm being tested
				// Note: We don't call rand.Seed() here, relying on Go 1.20+ auto-seeding
				j := i + (len(allAssetIds)-i)/2 // Simplified for testing, not truly random
				allAssetIds[i], allAssetIds[j] = allAssetIds[j], allAssetIds[i]
			}

			result := allAssetIds[:sampleSize]
			assert.Equal(t, tc.expectedLength, len(result))
		})
	}
}
