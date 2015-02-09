package tests

import (
	"git.encryptio.com/kvl"
)

func clearDB(s kvl.DB) error {
	_, err := s.RunTx(func(ctx kvl.Ctx) (interface{}, error) {
		for {
			pairs, err := ctx.Range(kvl.RangeQuery{Limit: 1000})
			if err != nil {
				return nil, err
			}

			if len(pairs) == 0 {
				return nil, nil
			}

			for _, pair := range pairs {
				err := ctx.Delete(pair.Key)
				if err != nil {
					return nil, err
				}
			}
		}
	})
	return err
}
