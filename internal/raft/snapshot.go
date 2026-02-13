package dekvsraft

import (
	"encoding/json"

	"github.com/hashicorp/raft"
)

type FSSnapshot struct {
	data map[string][]byte
}

func (f *FSSnapshot) Persist(sink raft.SnapshotSink) error {
	err := func() error {
		b, err := json.Marshal(f.data)
		if err != nil {
			return err
		}
		if _, err := sink.Write(b); err != nil {
			return err
		}
		return nil
	}()
	if err != nil {
		sink.Cancel()
		return err
	}

	sink.Close()
	return nil
}

func (f *FSSnapshot) Release() {}
