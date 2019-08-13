package cqrs

import (
	"errors"
	"io"
	"os"
)

//Other snapshot strategies to consider:
//JsonDumpBackup creates a JSON dump of the aggregate data and
// saves to the backup folder specified in the service settings.

//NoSnapshot utility  for aggreagates that don't need snapshots.
func NoSnapshot(aggregateName string) (BackupStrategy, RestoreStrategy) {
	backup := func(name string) (err error) {
		return errors.New("no backup strategy")
	}
	restore := func(name string) (err error) {
		return errors.New("no restore strategy")
	}
	return backup, restore
}

//FileCopyBackup snapshot strategy that users file copy to backup and restore the data.
func FileCopyBackup(dbBaseFolder, bkpBaseFolder string) SnapshotStrategy {
	return func(aggregateName string) (BackupStrategy, RestoreStrategy) {
		dbFolder := dbBaseFolder + "/" + aggregateName

		backup := func(snapshotID string) (err error) {
			bkpFolder := bkpBaseFolder + "/" + snapshotID + "/" + aggregateName
			return copyFolder(dbFolder, bkpFolder)
		}
		restore := func(snapshotID string) (err error) {
			bkpFolder := bkpBaseFolder + "/" + snapshotID + "/" + aggregateName
			return copyFolder(bkpFolder, dbFolder)
		}
		return backup, restore
	}
}

// copyFolder source to destination :)
func copyFolder(source string, dest string) (err error) {
	sourceInfo, err := os.Stat(source)
	if err != nil {
		return err
	}
	err = os.MkdirAll(dest, sourceInfo.Mode())
	if err != nil {
		return err
	}
	directory, _ := os.Open(source)
	objects, err := directory.Readdir(-1)
	for _, obj := range objects {
		sourcePointer := source + "/" + obj.Name()
		destinationPointer := dest + "/" + obj.Name()
		if obj.IsDir() {
			err = copyFolder(sourcePointer, destinationPointer)
			if err != nil {
				return err
			}
		} else {
			sourceFile, err := os.Open(sourcePointer)
			if err != nil {
				return err
			}
			defer sourceFile.Close()
			destfile, err := os.Create(destinationPointer)
			if err != nil {
				return err
			}
			defer destfile.Close()
			_, err = io.Copy(destfile, sourceFile)
			if err == nil {
				sourceInfo, err := os.Stat(source)
				if err != nil {
					err = os.Chmod(dest, sourceInfo.Mode())
				}
			}
		}
	}
	return nil
}
