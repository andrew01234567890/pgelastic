/*
Copyright 2026.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package pgbackrest

import (
	"encoding/json"
	"fmt"
	"time"
)

// Backup is one entry of the repository catalogue.
//
// Every field here is read back out of the repository after the backup completes rather
// than measured while it ran. The repository is the source of truth about what is in it,
// and a status assembled from what the command reported can disagree with what actually
// landed - which is a disagreement discovered at restore time.
type Backup struct {
	// Label is pgBackRest's name for the backup and what a restore is pointed at.
	Label string
	// Type is full, diff or incr. It is not always what was asked for: pgBackRest promotes
	// a differential or an incremental to a full when there is no full to descend from.
	Type BackupType
	// Started and Stopped are the backup's own times.
	Started time.Time
	Stopped time.Time
	// BeginLSN and EndLSN bound the WAL a restore has to replay to reach consistency, and
	// BeginWAL and EndWAL name the segments those positions fall in.
	BeginLSN string
	EndLSN   string
	BeginWAL string
	EndWAL   string
	// SizeBytes is what the backup occupies in the repository, after compression, rather
	// than the size of the database it was taken from.
	SizeBytes int64
	// SystemIdentifier is the database this backup belongs to, which is the same thing the
	// stanza is named after.
	SystemIdentifier string
	// Annotation carries whatever the backup was tagged with, which for backups this
	// operator took includes the PgBackup that asked for it.
	Annotation map[string]string
}

// catalogue mirrors the shape of `pgbackrest info --output=json`, and only the parts acted
// on. Fields nobody reads are deliberately absent: a struct that mirrored the whole document
// would have to be revised on every pgBackRest release for no benefit.
type catalogue struct {
	Name   string `json:"name"`
	Backup []struct {
		Label     string `json:"label"`
		Type      string `json:"type"`
		Timestamp struct {
			Start int64 `json:"start"`
			Stop  int64 `json:"stop"`
		} `json:"timestamp"`
		LSN struct {
			Start string `json:"start"`
			Stop  string `json:"stop"`
		} `json:"lsn"`
		Archive struct {
			Start string `json:"start"`
			Stop  string `json:"stop"`
		} `json:"archive"`
		Info struct {
			Repository struct {
				Size  int64 `json:"size"`
				Delta int64 `json:"delta"`
			} `json:"repository"`
		} `json:"info"`
		Database struct {
			ID int `json:"id"`
		} `json:"database"`
		Annotation map[string]string `json:"annotation"`
	} `json:"backup"`
	DB []struct {
		ID       int    `json:"id"`
		SystemID uint64 `json:"system-id"`
	} `json:"db"`
}

// ParseInfo reads the catalogue for one stanza out of `pgbackrest info --output=json`.
//
// The backups come back oldest first, which is pgBackRest's own order and the one retention
// reasons in.
func ParseInfo(document, stanza string) ([]Backup, error) {
	var stanzas []catalogue
	if err := json.Unmarshal([]byte(document), &stanzas); err != nil {
		return nil, fmt.Errorf("reading the repository catalogue: %w", err)
	}
	for _, entry := range stanzas {
		if entry.Name != stanza {
			continue
		}
		// The system identifier is per database history rather than per backup: pgBackRest
		// keys each backup to a db entry, and a stanza can hold more than one after an
		// upgrade. Resolving it here means a backup's status records the database it can
		// actually be restored into.
		systemIdentifiers := make(map[int]string, len(entry.DB))
		for _, database := range entry.DB {
			systemIdentifiers[database.ID] = fmt.Sprint(database.SystemID)
		}

		backups := make([]Backup, 0, len(entry.Backup))
		for _, item := range entry.Backup {
			backups = append(backups, Backup{
				Label:            item.Label,
				Type:             BackupType(item.Type),
				Started:          time.Unix(item.Timestamp.Start, 0).UTC(),
				Stopped:          time.Unix(item.Timestamp.Stop, 0).UTC(),
				BeginLSN:         item.LSN.Start,
				EndLSN:           item.LSN.Stop,
				BeginWAL:         item.Archive.Start,
				EndWAL:           item.Archive.Stop,
				SizeBytes:        item.Info.Repository.Size,
				SystemIdentifier: systemIdentifiers[item.Database.ID],
				Annotation:       item.Annotation,
			})
		}
		return backups, nil
	}
	return nil, fmt.Errorf("the repository holds no stanza named %q", stanza)
}

// FindByLabel returns one backup of the catalogue.
func FindByLabel(backups []Backup, label string) (Backup, bool) {
	for _, backup := range backups {
		if backup.Label == label {
			return backup, true
		}
	}
	return Backup{}, false
}

// Latest returns the most recently completed backup, which is the one a restore with no
// explicit target starts from.
func Latest(backups []Backup) (Backup, bool) {
	if len(backups) == 0 {
		return Backup{}, false
	}
	latest := backups[0]
	for _, backup := range backups[1:] {
		if backup.Stopped.After(latest.Stopped) {
			latest = backup
		}
	}
	return latest, true
}
