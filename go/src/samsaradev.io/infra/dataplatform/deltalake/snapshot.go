package deltalake

import (
	"sort"
	"time"

	"github.com/samsarahq/go/oops"

	"samsaradev.io/helpers/samtime"
)

type StreamingOffset struct {
	Version int64
	Index   int64
}

type File struct {
	Path             string
	Size             int64
	ModificationTime time.Time
}

// Files returns files referenced in the latest version.
func (log *DeltaLog) Files() []File {
	m := make(map[string]File)
	for _, version := range log.History {
		for _, action := range version.Actions {
			switch {
			case action.Add != nil:
				m[action.Add.Path] = File{
					Path:             action.Add.Path,
					Size:             action.Add.Size,
					ModificationTime: samtime.MsToTime(action.Add.ModificationTime),
				}
			case action.Remove != nil:
				delete(m, action.Remove.Path)
			}
		}
	}
	files := make([]File, 0, len(m))
	for _, file := range m {
		files = append(files, file)
	}
	sort.Slice(files, func(i, j int) bool {
		return files[i].Path < files[j].Path
	})
	return files
}

// StreamingFiles returns files added since a streaming offset.
// It ignores files rewritten in compaction, and files that are deleted.
func (log *DeltaLog) StreamingFiles(since StreamingOffset) []File {
	var files []File
	for _, version := range log.History {
		if version.Version < since.Version {
			continue
		}

		idx := int64(0)
		for _, action := range version.Actions {
			if action.Add == nil {
				// Stream source only applies to newly added files.
				continue
			}
			if action.Add.DataChange == false {
				// Ignore files in compaction.
				continue
			}

			if version.Version >= since.Version || (version.Version == since.Version && idx >= since.Index) {
				files = append(files, File{
					Path:             action.Add.Path,
					Size:             action.Add.Size,
					ModificationTime: samtime.MsToTime(action.Add.ModificationTime),
				})
			}

			idx++
		}
	}
	return files
}

func (log *DeltaLog) Metadata() (*ChangeMetadata, error) {
	for i := len(log.History) - 1; i >= 0; i-- {
		for _, action := range log.History[i].Actions {
			if action.MetaData != nil {
				return action.MetaData, nil
			}
		}
	}
	return nil, oops.Errorf("metadata not found")
}

func (log *DeltaLog) GetVersion(v int64) (*DeltaLogVersion, error) {
	for i := len(log.History) - 1; i >= 0; i-- {
		if log.History[i].Version == v {
			return log.History[i], nil
		}
	}

	if len(log.History) == 0 {
		return nil, oops.Errorf("version %d not found in [no version history, %d]", v, log.Version)
	}

	return nil, oops.Errorf("version %d not found in [%d, %d]", v, log.History[0].Version, log.Version)
}

func (v *DeltaLogVersion) CommitInfo() (*CommitInfo, error) {
	for _, action := range v.Actions {
		if action.CommitInfo != nil {
			return action.CommitInfo, nil
		}
	}
	return nil, oops.Errorf("commitInfo not found in version %d", v.Version)
}
