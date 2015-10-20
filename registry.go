package kvl

import (
	"sync"
	"errors"
)

var registry = make(map[string]func(string) (DB, error))
var registryMutex sync.Mutex

func Open(db, dsn string) (DB, error) {
	registryMutex.Lock()
	constructor, ok := registry[db]
	if !ok {
		registryMutex.Unlock()
		return nil, errors.New("kvl backend not registered")
	}
	registryMutex.Unlock()

	return constructor(dsn)
}

func RegisterBackend(db string, constructor func(string) (DB, error)) {
	registryMutex.Lock()
	if _, ok := registry[db]; ok {
		panic("backend already registered")
	}
	registry[db] = constructor
	registryMutex.Unlock()
}
