package proto

import "github.com/go-faster/errors"

type ServerData struct {
	TableName string
	Block     Block
}

func (d *ServerData) DecodeAware(r *Reader, revision int) error {
	if FeatureTempTables.In(revision) {
		v, err := r.Str()
		if err != nil {
			return errors.Wrap(err, "temp table")
		}
		d.TableName = v
	}
	if err := d.Block.DecodeAware(r, revision); err != nil {
		return errors.Wrap(err, "block")
	}

	return nil
}
