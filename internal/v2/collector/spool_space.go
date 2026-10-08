package collector

// JSON sizes exclude block rounding, directory metadata and inodes.
type spoolDiskSpace struct {
	available, total   uint64
	freeInodes, inodes uint64
}

func (s spoolDiskSpace) canWrite(bytes uint64) bool {
	reserve := max(uint64(64<<20), s.total/20)
	needed := bytes + (64 << 10)
	if s.available <= reserve || needed >= s.available-reserve {
		return false
	}
	return s.inodes == 0 || s.freeInodes > max(uint64(128), s.inodes/100)
}
