package collector

import "syscall"

func readSpoolDiskSpace(path string) (spoolDiskSpace, error) {
	var stat syscall.Statfs_t
	if err := syscall.Statfs(path, &stat); err != nil {
		return spoolDiskSpace{}, err
	}
	return spoolDiskSpace{available: stat.Bavail * uint64(stat.Bsize), total: stat.Blocks * uint64(stat.Bsize), freeInodes: stat.Ffree, inodes: stat.Files}, nil
}
