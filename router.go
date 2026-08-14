package main

import (
	"fmt"
	"log"
	"net"
	"sort"
	"strings"
	"sync"
	"time"
)

// DBStatus - состояние одной БД
type DBStatus struct {
	Config    DatabaseConfig
	Healthy   bool
	LastCheck time.Time
	mu        sync.RWMutex
}

// Router - отвечает за выбор БД
type Router struct {
	dbStatuses []*DBStatus
	roundRobin map[int]int
	rrMu       sync.Mutex
	healthStop chan struct{}
	config     Config
}

// NewRouter - создаёт новый роутер
func NewRouter(config Config) *Router {
	r := &Router{
		config:     config,
		healthStop: make(chan struct{}),
		roundRobin: make(map[int]int),
		dbStatuses: make([]*DBStatus, 0, len(config.Databases)),
	}

	// Инициализируем статусы БД
	for _, db := range config.Databases {
		r.dbStatuses = append(r.dbStatuses, &DBStatus{
			Config:    db,
			Healthy:   true,
			LastCheck: time.Now(),
		})
	}

	// Запускаем фоновую проверку здоровья
	go r.healthChecker()

	return r
}

// Stop - останавливает роутер
func (r *Router) Stop() {
	close(r.healthStop)
}

// SelectDatabase - выбирает БД на основе приоритета и round-robin
func (r *Router) SelectDatabase(dbName string) (*DatabaseConfig, error) {
	log.Printf("Selecting database for '%s'", dbName)

	// Получаем список БД, которые соответствуют запрошенному имени
	candidates := r.getCandidates(dbName)
	if len(candidates) == 0 {
		return nil, fmt.Errorf("no database found for '%s'", dbName)
	}

	// Группируем по приоритету
	priorityGroups := r.groupByPriority(candidates)

	// Идём по приоритетам (от меньшего к большему)
	for _, priority := range r.getSortedPriorities(priorityGroups) {
		group := priorityGroups[priority]

		// Фильтруем здоровые БД
		healthy := r.filterHealthy(group)
		if len(healthy) == 0 {
			log.Printf("All databases with priority %d are unhealthy, trying next priority", priority)
			continue
		}

		// Выбираем через round-robin
		selected := r.selectRoundRobin(priority, healthy)
		if selected != nil {
			log.Printf("Selected database %s (priority: %d, healthy: %v)",
				selected.Config.URL, selected.Config.Priority, selected.Healthy)
			return &selected.Config, nil
		}
	}

	return nil, fmt.Errorf("no healthy databases available")
}

// getCandidates - возвращает БД, соответствующие запрошенному имени
func (r *Router) getCandidates(dbName string) []*DBStatus {
	candidates := make([]*DBStatus, 0)
	for _, status := range r.dbStatuses {
		if dbName == "" || strings.Contains(status.Config.URL, dbName) {
			candidates = append(candidates, status)
		}
	}
	return candidates
}

// groupByPriority - группирует БД по приоритету
func (r *Router) groupByPriority(statuses []*DBStatus) map[int][]*DBStatus {
	groups := make(map[int][]*DBStatus)
	for _, status := range statuses {
		priority := status.Config.Priority
		groups[priority] = append(groups[priority], status)
	}
	return groups
}

// getSortedPriorities - возвращает отсортированные приоритеты
func (r *Router) getSortedPriorities(groups map[int][]*DBStatus) []int {
	priorities := make([]int, 0, len(groups))
	for priority := range groups {
		priorities = append(priorities, priority)
	}
	sort.Ints(priorities)
	return priorities
}

// filterHealthy - возвращает только здоровые БД
func (r *Router) filterHealthy(statuses []*DBStatus) []*DBStatus {
	healthy := make([]*DBStatus, 0, len(statuses))
	for _, status := range statuses {
		status.mu.RLock()
		isHealthy := status.Healthy
		status.mu.RUnlock()
		if isHealthy {
			healthy = append(healthy, status)
		}
	}
	return healthy
}

// selectRoundRobin - выбирает БД по round-robin внутри приоритета
func (r *Router) selectRoundRobin(priority int, healthy []*DBStatus) *DBStatus {
	if len(healthy) == 0 {
		return nil
	}

	r.rrMu.Lock()
	defer r.rrMu.Unlock()

	idx, exists := r.roundRobin[priority]
	if !exists {
		idx = 0
	}

	selected := healthy[idx%len(healthy)]
	r.roundRobin[priority] = (idx + 1) % len(healthy)

	return selected
}

// healthChecker - периодически проверяет доступность БД
func (r *Router) healthChecker() {
	ticker := time.NewTicker(10 * time.Second)
	defer ticker.Stop()

	log.Println("Health checker started (interval: 10s)")

	for {
		select {
		case <-ticker.C:
			r.checkAllDatabases()
		case <-r.healthStop:
			log.Println("Health checker stopped")
			return
		}
	}
}

// checkAllDatabases - проверяет все БД
func (r *Router) checkAllDatabases() {
	var wg sync.WaitGroup
	wg.Add(len(r.dbStatuses))

	for _, status := range r.dbStatuses {
		go func(s *DBStatus) {
			defer wg.Done()
			r.checkDatabase(s)
		}(status)
	}

	wg.Wait()
}

// checkDatabase - проверяет одну БД
func (r *Router) checkDatabase(status *DBStatus) {
	dsn := buildDSN(status.Config)
	hostPort := extractHostPort(dsn)

	// Пытаемся подключиться с тайм-аутом
	conn, err := net.DialTimeout("tcp", hostPort, 2*time.Second)
	if err != nil {
		status.mu.Lock()
		if status.Healthy {
			log.Printf("Database %s became UNHEALTHY: %v", status.Config.URL, err)
		}
		status.Healthy = false
		status.mu.Unlock()
		return
	}
	defer func() {
		if err := conn.Close(); err != nil {
			log.Printf("Error closing connection to %s: %v", hostPort, err)
		}
	}()

	// Проверяем, что БД отвечает
	if err := conn.SetDeadline(time.Now().Add(2 * time.Second)); err != nil {
		log.Printf("Error setting deadline on connection to %s: %v", hostPort, err)
	}

	status.mu.Lock()
	if !status.Healthy {
		log.Printf("Database %s became HEALTHY", status.Config.URL)
	}
	status.Healthy = true
	status.LastCheck = time.Now()
	status.mu.Unlock()
}
