package collector

import "golang.org/x/sys/windows"

func readSpoolDiskSpace(path string) (spoolDiskSpace, error) {
	p, err := windows.UTF16PtrFromString(path)
	if err != nil {
		return spoolDiskSpace{}, err
	}
	var space spoolDiskSpace
	err = windows.GetDiskFreeSpaceEx(p, &space.available, &space.total, nil)
	return space, err
}
