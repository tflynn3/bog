package main

import (
	"errors"
	"os"
	"slices"

	"bog/internal/checkpoint"
)

func writeGeneration(path string, blocks [][]checkpoint.BlockRow, l layout) (_ int64, err error) {
	var encoder blockEncoder
	defer func() { err = errors.Join(err, encoder.close()) }()
	return writeGenerationUsing(path, blocks, l, &encoder)
}

func writeGenerationUsing(path string, blocks [][]checkpoint.BlockRow, l layout, encoder *blockEncoder) (_ int64, err error) {
	f, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0600)
	if err != nil {
		return 0, err
	}
	defer func() { err = errors.Join(err, f.Close()) }()
	if err := encoder.write(f, l, len(blocks), func(i int) ([]byte, summary, error) {
		rows := slices.Clone(blocks[i])
		slices.SortFunc(rows, ordered)
		var filter summary
		if l.prune {
			for _, row := range rows {
				filter.add(row.Row.Line)
			}
		}
		raw, err := checkpoint.EncodeBlock(rows)
		return raw, filter, err
	}); err != nil {
		return 0, err
	}
	if err := f.Sync(); err != nil {
		return 0, err
	}
	info, err := f.Stat()
	if err != nil {
		return 0, err
	}
	return info.Size(), nil
}
