package ram

type watcher struct {
	db    *DB
	locks locks
	done  chan struct{}
}

func (db *DB) newWatcher(locks locks) *watcher {
	return &watcher{
		db:    db,
		locks: locks,
		done:  make(chan struct{}),
	}
}

func (w *watcher) Done() <-chan struct{} {
	return w.done
}

func (w *watcher) Error() error {
	return nil
}

func (w *watcher) Close() {
	select {
	case <-w.done:
		return
	default:
		close(w.done)
		w.db.removeWatcher(w)
	}
}

func (w *watcher) trigger() {
	select {
	case <-w.done:
	default:
		close(w.done)
		// our caller will remove the watcher from the db
	}
}
