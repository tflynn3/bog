package main

import (
	"crypto/rand"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
)

func writeRunnerReport(path string, result any) (err error) {
	state := "replacement not published"
	defer func() {
		if err != nil {
			err = fmt.Errorf("write report %q: %s: %w", path, state, err)
		}
	}()
	data, err := json.MarshalIndent(result, "", "  ")
	if err != nil {
		return err
	}
	parent, name := filepath.Dir(path), filepath.Base(path)
	if name == "." || name == ".." || name == string(filepath.Separator) || filepath.Clean(path) != path {
		return errors.New("require a clean report file path")
	}
	root, err := os.OpenRoot(parent)
	if err != nil {
		return err
	}
	defer func() { err = errors.Join(err, root.Close()) }()
	dir, err := root.Open(".")
	if err != nil {
		return err
	}
	defer func() { err = errors.Join(err, dir.Close()) }()

	staged := ".release-report-" + rand.Text()
	file, err := root.OpenFile(staged, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0600)
	if err != nil {
		return err
	}
	renamed := false
	defer func() {
		if !renamed {
			err = errors.Join(err, root.Remove(staged))
		}
	}()
	data = append(data, '\n')
	n, err := file.Write(data)
	if err == nil && n != len(data) {
		err = io.ErrShortWrite
	}
	if err == nil {
		err = file.Sync()
	}
	if err = errors.Join(err, file.Close()); err != nil {
		return err
	}
	if err = root.Rename(staged, name); err != nil {
		return err
	}
	renamed = true
	state = "replacement published, directory durability uncertain"
	if err = dir.Sync(); err != nil {
		return err
	}
	state = "replacement published and synced"
	return nil
}
