package kvdb

// TotalStats returns the total statistics of all spaces.
func (db *T) TotalStats() Stats {
	db.mu.RLock()
	defer db.mu.RUnlock()

	return db.totalStats()
}

/******************
 * Statistics
 */

type Stats struct {
	Alive    uint64
	Dead     uint64
	AlivePct float64
}

func (db *T) totalStats() Stats {
	total := Stats{}
	for _, v := range db.spaces {
		total.Alive += v.Alive()
		total.Dead += v.Dead()
	}
	total.AlivePct = float64(total.Alive+1) / float64(total.Alive+1+total.Dead)
	return total
}
