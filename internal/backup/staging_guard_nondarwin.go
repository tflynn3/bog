//go:build !darwin

package backup

func readStagingLink(c stageClaim, name string, _ int) (string, error) {
	return c.parent.Readlink(name)
}
