package inst

import (
	"fmt"
	"time"

	"github.com/openark/golib/log"
)

// https://github.com/openark/orchestrator/issues/1430

// StartAndWaitForSQLThreadUpToDate starts a replica
// It will actually START the sql_thread even if the replica is completely stopped.
// And will wait for sql thread up to date
func StartAndWaitForSQLThreadUpToDate(instanceKey *InstanceKey, overallTimeout time.Duration, staleCoordinatesTimeout time.Duration) (*Instance, error) {
	instance, err := ReadTopologyInstance(instanceKey)
	if err != nil {
		return instance, log.Errore(err)
	}

	if !instance.ReplicationThreadsExist() {
		return instance, fmt.Errorf("instance is not a replica: %+v", instanceKey)
	}

	// start sql_thread but catch any errors
	for _, cmd := range []string{`start slave sql_thread`} {
		if _, err := ExecInstance(instanceKey, cmd); err != nil {
			return nil, log.Errorf("%+v: StartReplicationAndWaitForSQLThreadUpToDate: '%q' failed: %+v", *instanceKey, cmd, err)
		}
	}

	if instance.SQLDelay == 0 {
		// Otherwise we don't bother.
		if instance, err = WaitForSQLThreadUpToDate(instanceKey, overallTimeout, staleCoordinatesTimeout); err != nil {
			return instance, err
		}
	}

	instance, err = ReadTopologyInstance(instanceKey)
	log.Infof("Started replication and wait for sql_thread up to date on %+v, Read:%+v, Exec:%+v", *instanceKey, instance.ReadBinlogCoordinates, instance.ExecBinlogCoordinates)
	return instance, err
}
