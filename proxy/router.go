package proxy

import (
	"fmt"
	"hash/fnv"
	"log/slog"
	"net"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/jackc/pgx/v5/pgproto3"
)

type DBStatus struct {
	Config    DatabaseConfig
	Healthy   bool
	LastCheck time.Time
	mu        sync.RWMutex
}

type Router struct {
	dbStatuses []*DBStatus
	roundRobin map[int]int
	algorithm  string // "ip-hash" или "round-robin"
	rrMu       sync.Mutex
	stopCh     chan struct{}
}

func NewRouter(dbs []DatabaseConfig, algorithm string) *Router {
	if algorithm == "" {
		algorithm = "ip-hash"
	}
	r := &Router{
		stopCh:     make(chan struct{}),
		roundRobin: make(map[int]int),
		algorithm:  strings.ToLower(algorithm),
		dbStatuses: make([]*DBStatus, 0, len(dbs)),
	}

	for _, db := range dbs {
		r.dbStatuses = append(r.dbStatuses, &DBStatus{
			Config:    db,
			Healthy:   true,
			LastCheck: time.Now(),
		})
	}

	go r.healthCheckerLoop()
	return r
}

func (r *Router) Stop() {
	close(r.stopCh)
}

// selectDatabase выбирает ноду с учётом приоритета, статуса здоровья и IP клиента
func (r *Router) selectDatabase(clientAddr string) (*DatabaseConfig, error) {
	groups := make(map[int][]*DBStatus)
	for _, st := range r.dbStatuses {
		groups[st.Config.Priority] = append(groups[st.Config.Priority], st)
	}

	priorities := make([]int, 0, len(groups))
	for p := range groups {
		priorities = append(priorities, p)
	}
	sort.Ints(priorities)

	for _, p := range priorities {
		group := groups[p]
		healthy := make([]*DBStatus, 0, len(group))
		for _, st := range group {
			st.mu.RLock()
			isH := st.Healthy
			st.mu.RUnlock()
			if isH {
				healthy = append(healthy, st)
			}
		}

		if len(healthy) == 0 {
			continue // Все ноды этого приоритета недоступны, переходим к следующему приоритету (Failover)
		}

		// IP-Hash: гарантирует, что один клиент (DBeaver, пул HikariCP) всегда работает внутри одной ноды
		if r.algorithm == "ip-hash" && clientAddr != "" {
			clientIP := extractIP(clientAddr)
			idx := hashIP(clientIP, len(healthy))
			return &healthy[idx].Config, nil
		}

		// Round-Robin
		r.rrMu.Lock()
		idx := r.roundRobin[p]
		selected := healthy[idx%len(healthy)]
		r.roundRobin[p] = (idx + 1) % len(healthy)
		r.rrMu.Unlock()

		return &selected.Config, nil
	}

	return nil, fmt.Errorf("no healthy databases available")
}

// healthCheckerLoop запускает проверки бд
func (r *Router) healthCheckerLoop() {
	ticker := time.NewTicker(10 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			var wg sync.WaitGroup
			for _, status := range r.dbStatuses {
				wg.Add(1)
				go func(s *DBStatus) {
					defer wg.Done()
					r.checkDBHealth(s)
				}(status)
			}
			wg.Wait()
		case <-r.stopCh:
			return
		}
	}
}

// checkDBHealth проверяет бд тестовым коннектом
func (r *Router) checkDBHealth(s *DBStatus) {
	conn, err := net.DialTimeout("tcp", s.Config.HostPort, 2*time.Second)
	if err != nil {
		r.updateStatus(s, false, err)
		return
	}
	defer conn.Close()

	_ = conn.SetDeadline(time.Now().Add(2 * time.Second))

	startup := buildStartupMessage(nil, s.Config.Login, s.Config.DBName)
	if _, err := conn.Write(startup); err != nil {
		r.updateStatus(s, false, err)
		return
	}

	frontend := pgproto3.NewFrontend(conn, conn)
	msg, err := frontend.Receive()
	if err != nil {
		r.updateStatus(s, false, err)
		return
	}

	switch msg.(type) {
	case *pgproto3.AuthenticationOk,
		*pgproto3.AuthenticationCleartextPassword,
		*pgproto3.AuthenticationMD5Password,
		*pgproto3.AuthenticationSASL:
		r.updateStatus(s, true, nil)
	default:
		if errResp, ok := msg.(*pgproto3.ErrorResponse); ok {
			r.updateStatus(s, false, fmt.Errorf("pg error: %s", errResp.Message))
		} else {
			r.updateStatus(s, true, nil)
		}
	}
}

// updateStatus обновляет статус бд
func (r *Router) updateStatus(s *DBStatus, healthy bool, err error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.Healthy != healthy {
		if healthy {
			slog.Info("Backend recovered to HEALTHY", "backend", s.Config.HostPort)
		} else {
			slog.Warn("Backend became UNHEALTHY", "backend", s.Config.HostPort, "err", err)
		}
	}

	s.Healthy = healthy
	s.LastCheck = time.Now()
}

// extractHostPort парсит сетевой адрес целевой бд
func extractHostPort(url string) string {
	d := url
	for _, prefix := range []string{"jdbc:postgresql://", "postgres://", "postgresql://"} {
		if idx := len(prefix); len(d) >= idx && d[:idx] == prefix {
			d = d[idx:]
			break
		}
	}
	if at := strings.IndexByte(d, '@'); at != -1 {
		d = d[at+1:]
	}
	if slash := strings.IndexByte(d, '/'); slash != -1 {
		d = d[:slash]
	}
	if q := strings.IndexByte(d, '?'); q != -1 {
		d = d[:q]
	}
	return d
}

// extractDBName получает имя бд
func extractDBName(url string) string {
	d := url
	for _, prefix := range []string{"jdbc:postgresql://", "postgres://", "postgresql://"} {
		if idx := len(prefix); len(d) >= idx && d[:idx] == prefix {
			d = d[idx:]
			break
		}
	}
	slash := strings.IndexByte(d, '/')
	if slash == -1 {
		return "postgres"
	}
	rest := d[slash+1:]
	if q := strings.IndexByte(rest, '?'); q != -1 {
		rest = rest[:q]
	}
	if rest == "" {
		return "postgres"
	}
	return rest
}

// extractIP отсекает клиентский эфемерный порт
func extractIP(addr string) string {
	host, _, err := net.SplitHostPort(addr)
	if err != nil {
		return addr
	}
	return host
}

// hashIP вычисляет детерминированный индекс ноды по IP адресу
func hashIP(ip string, count int) int {
	if count <= 1 {
		return 0
	}
	h := fnv.New32a()
	_, _ = h.Write([]byte(ip))
	return int(h.Sum32()) % count
}
