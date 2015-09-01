package ram

// data is a linked list of map[string]*strings.
type data struct {
	contents map[string]*string
	refcount int
	inner    *data
}

func (d data) get(key string) *string {
	v, ok := d.contents[key]
	if ok {
		return v
	}

	if d.inner == nil {
		return nil
	}

	return d.inner.get(key)
}

func (d data) getRange(r keyRange) map[string]*string {
	m := make(map[string]*string)
	d.getRangeInto(r, m)
	return m
}

func (d data) getRangeInto(r keyRange, m map[string]*string) {
	if d.inner != nil {
		d.inner.getRangeInto(r, m)
	}

	for k, v := range d.contents {
		if k < r.low || (r.high != "" && k >= r.high) {
			continue
		}

		if v == nil {
			delete(m, k)
		} else {
			m[k] = v
		}
	}
}

type locks struct {
	keys   []string
	ranges []keyRange
}

func (l locks) conflicts(d *data) bool {
	for _, k := range l.keys {
		_, found := d.contents[k]
		if found {
			return true
		}
	}

	for _, r := range l.ranges {
		for k := range d.contents {
			if k >= r.low && (r.high == "" || k < r.high) {
				return true
			}
		}
	}

	return false
}
