package proto

import "testing"

func TestClientData_EncodeAware(t *testing.T) {
	Gold(t, ClientData{
		Block: Block{
			Info: BlockInfo{
				BucketNum: -1,
			},
			Columns: 10,
			Rows:    15,
		},
	})
}
