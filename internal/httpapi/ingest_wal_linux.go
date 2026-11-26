package httpapi

import (
	"errors"

	"bog/internal/wal"
)

func isUncertainAppend(err error) bool {
	return errors.Is(err, wal.ErrUncertain)
}
