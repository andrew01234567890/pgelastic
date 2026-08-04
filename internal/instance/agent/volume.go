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

package agent

import "syscall"

// WAL headroom a member must have before it is allowed to be promoted.
//
// Both floors apply, whichever is larger. The absolute floor exists because a percentage of
// a small volume is not a number of WAL segments, and PostgreSQL's unit of allocation is a
// segment; the proportional floor exists because a large volume needs room for a checkpoint
// and for whatever wal_keep_size and the slots are retaining. Promoting onto a volume with
// less than this buys a primary that PANICs on its first checkpoint and takes every tenant
// on the instance with it.
const (
	// WALHeadroomSegments is the absolute floor, in 16 MiB segments.
	WALHeadroomSegments = 8
	// WALSegmentBytes is the segment size pgelastic pins at initdb.
	WALSegmentBytes = 16 << 20
	// WALHeadroomFraction is the proportional floor.
	WALHeadroomFraction = 0.05
)

// VolumeUsage is a filesystem's capacity as the agent measured it.
type VolumeUsage struct {
	// TotalBytes and FreeBytes are what the filesystem reports for an unprivileged user,
	// which is the budget the postgres user actually has rather than the raw device size.
	TotalBytes int64
	FreeBytes  int64
}

// UsedBytes is what the filesystem is holding, from the unprivileged figures.
//
// Total minus *available* rather than minus free, so the answer matches the budget Full() is
// judged against: the blocks a filesystem reserves for root are not room PostgreSQL can use,
// so counting them as free would report a volume as emptier than PostgreSQL can treat it.
func (u VolumeUsage) UsedBytes() int64 {
	if u.TotalBytes <= 0 {
		return 0
	}
	return u.TotalBytes - u.FreeBytes
}

// Full reports whether the volume has too little room left to promote onto.
func (u VolumeUsage) Full() bool {
	if u.TotalBytes <= 0 {
		return false
	}
	floor := int64(WALHeadroomSegments * WALSegmentBytes)
	if proportional := int64(float64(u.TotalBytes) * WALHeadroomFraction); proportional > floor {
		floor = proportional
	}
	return u.FreeBytes < floor
}

// MeasureVolume reads a mounted filesystem's capacity.
//
// The free figure is the unprivileged one (Bavail, not Bfree): the reserved blocks a
// filesystem keeps for root are not room PostgreSQL can write into, and counting them is
// how a volume reports free space right up to the moment the postmaster PANICs.
func MeasureVolume(path string) (VolumeUsage, error) {
	var stat syscall.Statfs_t
	if err := syscall.Statfs(path, &stat); err != nil {
		return VolumeUsage{}, err
	}
	blockSize := stat.Bsize
	return VolumeUsage{
		TotalBytes: int64(stat.Blocks) * blockSize,
		FreeBytes:  int64(stat.Bavail) * blockSize,
	}, nil
}
