package main

import (
	"fmt"
	"io"
	"log"
	"net"
	"strings"
	"sync"
	"time"
)

type ProxyServer struct {
	config     Config
	router     *Router
	listener   net.Listener
	wg         sync.WaitGroup
	shutdownCh chan struct{}
}

func NewProxyServer(config Config) *ProxyServer {
	router := NewRouter(config)

	return &ProxyServer{
		config:     config,
		router:     router,
		shutdownCh: make(chan struct{}),
	}
}

func (p *ProxyServer) Start() error {
	addr := fmt.Sprintf(":%d", p.config.Server.Port)
	listener, err := net.Listen("tcp", addr)
	if err != nil {
		return fmt.Errorf("failed to listen on %s: %w", addr, err)
	}
	p.listener = listener

	log.Printf("Proxy server listening on %s", addr)
	log.Printf("Target databases: %d", len(p.config.Databases))

	p.wg.Add(1)
	go p.acceptConnections()

	return nil
}

func (p *ProxyServer) Stop() {
	log.Println("Shutting down proxy server...")
	close(p.shutdownCh)

	if p.router != nil {
		p.router.Stop()
	}

	if p.listener != nil {
		if err := p.listener.Close(); err != nil {
			log.Printf("Failed to close listener: %s", err)
		}
	}

	done := make(chan struct{})
	go func() {
		p.wg.Wait()
		close(done)
	}()

	select {
	case <-done:
		log.Println("All connections closed gracefully")
	case <-time.After(30 * time.Second):
		log.Println("Shutdown timeout, forcing exit")
	}
}

func (p *ProxyServer) acceptConnections() {
	defer p.wg.Done()

	for {
		select {
		case <-p.shutdownCh:
			log.Println("Stopping accepting new connections")
			return
		default:
		}

		clientConn, err := p.listener.Accept()
		if err != nil {
			select {
			case <-p.shutdownCh:
				return
			default:
				log.Printf("Accept error: %v", err)
				continue
			}
		}

		log.Printf("New client connection from %s", clientConn.RemoteAddr())
		p.wg.Add(1)
		go p.handleClient(clientConn)
	}
}

func (p *ProxyServer) handleClient(clientConn net.Conn) {
	defer p.wg.Done()
	defer func() {
		if err := clientConn.Close(); err != nil {
			log.Printf("Failed to close client connection: %s", err)
		}
	}()

	clientAddr := clientConn.RemoteAddr().String()

	// Читаем StartupMessage
	clientParams, err := p.readStartupMessage(clientConn)
	if err != nil {
		log.Printf("Failed to read startup message from %s: %v", clientAddr, err)
		return
	}

	log.Printf("Client %s: user=%s, database=%s", clientAddr, clientParams["user"], clientParams["database"])

	// Проверяем логин
	if clientParams["user"] != p.config.Server.Login {
		log.Printf("Invalid user from %s: %s (expected: %s)", clientAddr, clientParams["user"], p.config.Server.Login)
		p.sendErrorResponse(clientConn, "invalid user")
		return
	}

	// Проверяем пароль
	if err := p.checkClientPassword(clientConn, p.config.Server.Pass); err != nil {
		log.Printf("Client authentication failed from %s: %v", clientAddr, err)
		return
	}

	// Выбираем БД через роутер
	targetDB, err := p.router.SelectDatabase(clientParams["database"])
	if err != nil {
		log.Printf("Failed to select database for %s: %v", clientAddr, err)
		p.sendErrorResponse(clientConn, err.Error())
		return
	}

	log.Printf("Routing %s to database: %s (priority: %d)",
		clientAddr, targetDB.URL, targetDB.Priority)

	// Создаём соединение к выбранной БД
	dsn := buildDSN(*targetDB)
	hostPort := extractHostPort(dsn)

	backendConn, err := net.Dial("tcp", hostPort)
	if err != nil {
		log.Printf("Failed to connect to backend %s: %v", targetDB.URL, err)
		p.sendErrorResponse(clientConn, "backend connection failed")
		return
	}
	defer func() {
		if err := backendConn.Close(); err != nil {
			log.Printf("Failed to close backend connection: %v", err)
		}
	}()

	// Проксируем StartupMessage на бэкенд
	backendParams := map[string]string{
		"user":     targetDB.Login,
		"database": extractDBName(targetDB.URL),
	}
	for key, value := range clientParams {
		if key != "user" && key != "database" {
			backendParams[key] = value
		}
	}

	if err := p.sendStartupMessage(backendConn, backendParams); err != nil {
		log.Printf("Failed to send startup to backend: %v", err)
		return
	}

	// Проксируем аутентификацию
	if err := p.proxyAuthentication(clientConn, backendConn, targetDB); err != nil {
		log.Printf("Authentication proxy failed: %v", err)
		return
	}

	log.Printf("Connection established for %s", clientAddr)

	// Проксируем данные
	var proxyWg sync.WaitGroup
	proxyWg.Add(2)

	go func() {
		defer proxyWg.Done()
		p.proxyData(clientConn, backendConn)
	}()

	go func() {
		defer proxyWg.Done()
		p.proxyData(backendConn, clientConn)
	}()

	proxyWg.Wait()
	log.Printf("Client %s disconnected", clientAddr)
}

func (p *ProxyServer) readStartupMessage(conn net.Conn) (map[string]string, error) {
	// Читаем заголовок
	header := make([]byte, 8)
	_, err := io.ReadFull(conn, header)
	if err != nil {
		return nil, fmt.Errorf("failed to read header: %w", err)
	}

	length := int32(header[0])<<24 | int32(header[1])<<16 | int32(header[2])<<8 | int32(header[3])

	// Проверяем SSL-запрос
	if length == 8 {
		sslCode := int32(header[4])<<24 | int32(header[5])<<16 | int32(header[6])<<8 | int32(header[7])
		if sslCode == 80877103 {
			log.Printf("SSL request from %s, denying", conn.RemoteAddr())
			if _, err := conn.Write([]byte{'N'}); err != nil {
				return nil, fmt.Errorf("failed to send SSL rejection: %w", err)
			}
			// Читаем StartupMessage после SSL-отказа
			return p.readStartupMessageAfterSSL(conn)
		}
	}

	// Читаем остаток StartupMessage
	data := make([]byte, length)
	copy(data[:8], header)
	_, err = io.ReadFull(conn, data[8:])
	if err != nil {
		return nil, fmt.Errorf("failed to read body: %w", err)
	}

	return p.parseStartupParams(data), nil
}

func (p *ProxyServer) readStartupMessageAfterSSL(conn net.Conn) (map[string]string, error) {
	lenBuf := make([]byte, 4)
	_, err := io.ReadFull(conn, lenBuf)
	if err != nil {
		return nil, fmt.Errorf("failed to read length: %w", err)
	}

	length := int32(lenBuf[0])<<24 | int32(lenBuf[1])<<16 | int32(lenBuf[2])<<8 | int32(lenBuf[3])
	if length < 8 {
		return nil, fmt.Errorf("invalid startup message length: %d", length)
	}

	data := make([]byte, length)
	copy(data[:4], lenBuf)

	_, err = io.ReadFull(conn, data[4:])
	if err != nil {
		return nil, fmt.Errorf("failed to read body: %w", err)
	}

	return p.parseStartupParams(data), nil
}

func (p *ProxyServer) parseStartupParams(data []byte) map[string]string {
	params := make(map[string]string)

	if len(data) < 8 {
		return params
	}

	pos := 8
	dataLen := len(data)

	for pos < dataLen {
		keyEnd := pos
		for keyEnd < dataLen && data[keyEnd] != 0 {
			keyEnd++
		}
		if keyEnd == pos || keyEnd >= dataLen {
			break
		}

		key := string(data[pos:keyEnd])
		pos = keyEnd + 1

		valEnd := pos
		for valEnd < dataLen && data[valEnd] != 0 {
			valEnd++
		}
		if valEnd == pos || valEnd >= dataLen {
			break
		}

		value := string(data[pos:valEnd])
		pos = valEnd + 1

		params[key] = value
	}

	return params
}

func (p *ProxyServer) sendStartupMessage(conn net.Conn, params map[string]string) error {
	log.Printf("Sending StartupMessage with params: user=%s, database=%s",
		params["user"], params["database"])
	data := make([]byte, 0, 256)

	version := []byte{0x00, 0x03, 0x00, 0x00}
	data = append(data, version...)

	for key, value := range params {
		data = append(data, []byte(key)...)
		data = append(data, 0)
		data = append(data, []byte(value)...)
		data = append(data, 0)
	}
	data = append(data, 0)

	length := 4 + len(data)
	lengthBytes := []byte{
		byte(length >> 24),
		byte(length >> 16),
		byte(length >> 8),
		byte(length),
	}

	if _, err := conn.Write(lengthBytes); err != nil {
		return fmt.Errorf("failed to send length: %w", err)
	}
	if _, err := conn.Write(data); err != nil {
		return fmt.Errorf("failed to send body: %w", err)
	}

	return nil
}

func (p *ProxyServer) checkClientPassword(conn net.Conn, expectedPassword string) error {
	if err := conn.SetDeadline(time.Now().Add(5 * time.Second)); err != nil {
		return fmt.Errorf("failed to set deadline: %w", err)
	}
	defer func() {
		if err := conn.SetDeadline(time.Time{}); err != nil {
			log.Printf("failed to set deadline: %v", err)
		}
	}()

	// Отправляем запрос пароля
	authRequest := []byte{'R', 0, 0, 0, 8, 0, 0, 0, 3}
	if _, err := conn.Write(authRequest); err != nil {
		return fmt.Errorf("failed to send auth request: %w", err)
	}

	// Читаем ответ клиента
	buf := make([]byte, 4096)
	n, err := conn.Read(buf)
	if err != nil {
		return fmt.Errorf("failed to read password: %w", err)
	}

	if n < 6 || buf[0] != 'p' {
		return fmt.Errorf("invalid password message")
	}

	// Находим пароль
	passwordStart := 5
	passwordEnd := passwordStart
	for passwordEnd < n && buf[passwordEnd] != 0 {
		passwordEnd++
	}

	if passwordEnd == passwordStart || passwordEnd >= n {
		return fmt.Errorf("no password found")
	}

	clientPassword := string(buf[passwordStart:passwordEnd])

	log.Printf("Client password received: '%s' (%d bytes)", clientPassword, len(clientPassword)) //TODO: убрать

	if clientPassword != expectedPassword {
		log.Printf("Invalid password from client (expected: %s, got: %s)", expectedPassword, clientPassword)
		p.sendErrorResponse(conn, "invalid password")
		return fmt.Errorf("invalid password")
	}

	log.Printf("Password verified successfully")
	return nil
}

func (p *ProxyServer) proxyAuthentication(clientConn net.Conn, backendConn net.Conn, targetDB *DatabaseConfig) error {
	clientConn.SetDeadline(time.Now().Add(10 * time.Second))
	backendConn.SetDeadline(time.Now().Add(10 * time.Second))
	defer clientConn.SetDeadline(time.Time{})
	defer backendConn.SetDeadline(time.Time{})

	buf := make([]byte, 4096)

	for {
		// 1. Читаем ответ от бэкенда
		n, err := backendConn.Read(buf)
		if err != nil {
			return fmt.Errorf("failed to read from backend: %w", err)
		}

		if n < 1 {
			continue
		}

		msgType := buf[0]
		log.Printf("Backend -> Client: type='%c' (%d bytes)", msgType, n)

		// 2. ErrorResponse
		if msgType == 'E' {
			log.Printf("Backend error: %s", string(buf[5:n]))
			return fmt.Errorf("backend auth error: %s", string(buf[5:n]))
		}

		// 3. AuthenticationOk
		if msgType == 'R' && n >= 9 {
			authCode := int32(buf[5])<<24 | int32(buf[6])<<16 | int32(buf[7])<<8 | int32(buf[8])

			if authCode == 0 {
				log.Printf("AuthenticationOk from backend")
				if _, err := clientConn.Write(buf[:n]); err != nil {
					return fmt.Errorf("failed to write auth ok to client: %w", err)
				}
				return nil
			}

			// 4. AuthenticationCleartextPassword (код 3)
			if authCode == 3 {
				log.Printf("Backend requests cleartext password")

				if _, err := clientConn.Write(buf[:n]); err != nil {
					return fmt.Errorf("failed to write auth request to client: %w", err)
				}

				n, err := clientConn.Read(buf)
				if err != nil {
					return fmt.Errorf("failed to read password from client: %w", err)
				}
				log.Printf("Client password consumed (len=%d), using DB password from config", n)

				dbPassword := targetDB.Pass
				passwordMsg := buildPasswordMessage(dbPassword)

				if _, err := backendConn.Write(passwordMsg); err != nil {
					return fmt.Errorf("failed to send DB password to backend: %w", err)
				}
				log.Printf("Sent DB password to backend (len=%d)", len(dbPassword))

				n, err = backendConn.Read(buf)
				if err != nil {
					return fmt.Errorf("failed to read auth response from backend: %w", err)
				}

				if _, err := clientConn.Write(buf[:n]); err != nil {
					return fmt.Errorf("failed to write auth response to client: %w", err)
				}

				if n >= 9 && buf[0] == 'R' {
					authCode = int32(buf[5])<<24 | int32(buf[6])<<16 | int32(buf[7])<<8 | int32(buf[8])
					if authCode == 0 {
						log.Printf("AuthenticationOk from backend after password")
						return nil
					}
				}

				return fmt.Errorf("authentication failed after password")
			}

			// 5. AuthenticationMD5Password (код 5)
			if authCode == 5 {
				log.Printf("Backend requests MD5 password")

				if _, err := clientConn.Write(buf[:n]); err != nil {
					return fmt.Errorf("failed to write MD5 challenge to client: %w", err)
				}

				n, err := clientConn.Read(buf)
				if err != nil {
					return fmt.Errorf("failed to read MD5 response from client: %w", err)
				}
				log.Printf("Client MD5 response consumed (len=%d)", n)

				dbPassword := targetDB.Pass
				passwordMsg := buildPasswordMessage(dbPassword)

				if _, err := backendConn.Write(passwordMsg); err != nil {
					return fmt.Errorf("failed to send DB password to backend: %w", err)
				}
				log.Printf("Sent DB password to backend for MD5")

				n, err = backendConn.Read(buf)
				if err != nil {
					return fmt.Errorf("failed to read auth response from backend: %w", err)
				}

				if _, err := clientConn.Write(buf[:n]); err != nil {
					return fmt.Errorf("failed to write auth response to client: %w", err)
				}

				if n >= 9 && buf[0] == 'R' {
					authCode = int32(buf[5])<<24 | int32(buf[6])<<16 | int32(buf[7])<<8 | int32(buf[8])
					if authCode == 0 {
						log.Printf("AuthenticationOk from backend after MD5")
						return nil
					}
				}

				return fmt.Errorf("authentication failed after MD5")
			}

			// 6. AuthenticationSASL (код 10) - SCRAM-SHA-256
			if authCode == 10 {
				log.Printf("Backend requests SASL authentication (SCRAM-SHA-256)")

				// 6a. Проксируем SASL-запрос клиенту
				if _, err := clientConn.Write(buf[:n]); err != nil {
					return fmt.Errorf("failed to write SASL request to client: %w", err)
				}

				// 6b. Читаем ответ клиента (SASLInitialResponse)
				n, err := clientConn.Read(buf)
				if err != nil {
					return fmt.Errorf("failed to read SASL response from client: %w", err)
				}
				log.Printf("Client SASL response consumed (len=%d)", n)

				// 6c. ✅ Отправляем бэкенду пароль из конфига (он сам выполнит SCRAM)
				dbPassword := targetDB.Pass
				passwordMsg := buildPasswordMessage(dbPassword)

				if _, err := backendConn.Write(passwordMsg); err != nil {
					return fmt.Errorf("failed to send DB password to backend: %w", err)
				}
				log.Printf("Sent DB password to backend for SASL (len=%d)", len(dbPassword))

				// 6d. Читаем ответ от бэкенда (AuthenticationSASLContinue или AuthenticationOk)
				n, err = backendConn.Read(buf)
				if err != nil {
					return fmt.Errorf("failed to read SASL response from backend: %w", err)
				}

				// 6e. Проксируем ответ клиенту
				if _, err := clientConn.Write(buf[:n]); err != nil {
					return fmt.Errorf("failed to write SASL response to client: %w", err)
				}

				// 6f. Проверяем, не завершена ли аутентификация
				if n >= 9 && buf[0] == 'R' {
					authCode = int32(buf[5])<<24 | int32(buf[6])<<16 | int32(buf[7])<<8 | int32(buf[8])
					if authCode == 0 {
						log.Printf("AuthenticationOk from backend after SASL")
						return nil
					}
				}

				// 6g. Если пришёл AuthenticationSASLContinue (код 11), нужно продолжить
				if n >= 9 && buf[0] == 'R' {
					authCode = int32(buf[5])<<24 | int32(buf[6])<<16 | int32(buf[7])<<8 | int32(buf[8])
					if authCode == 11 {
						log.Printf("Backend requests SASL continue, proxying...")

						// Проксируем continue клиенту
						if _, err := clientConn.Write(buf[:n]); err != nil {
							return fmt.Errorf("failed to write SASL continue to client: %w", err)
						}

						// Читаем ответ клиента
						n, err := clientConn.Read(buf)
						if err != nil {
							return fmt.Errorf("failed to read SASL continue from client: %w", err)
						}

						// Проксируем бэкенду
						if _, err := backendConn.Write(buf[:n]); err != nil {
							return fmt.Errorf("failed to write SASL continue to backend: %w", err)
						}

						// Читаем финальный ответ
						n, err = backendConn.Read(buf)
						if err != nil {
							return fmt.Errorf("failed to read final SASL response: %w", err)
						}

						if _, err := clientConn.Write(buf[:n]); err != nil {
							return fmt.Errorf("failed to write final SASL response to client: %w", err)
						}

						if n >= 9 && buf[0] == 'R' {
							authCode = int32(buf[5])<<24 | int32(buf[6])<<16 | int32(buf[7])<<8 | int32(buf[8])
							if authCode == 0 {
								log.Printf("AuthenticationOk from backend after SASL continue")
								return nil
							}
						}

						return fmt.Errorf("authentication failed after SASL continue")
					}
				}

				return fmt.Errorf("authentication failed after SASL")
			}

			// 7. Другие типы аутентификации
			log.Printf("Unsupported auth type: %d", authCode)
			return fmt.Errorf("unsupported auth type: %d", authCode)
		}

		// 8. Для других типов сообщений - проксируем
		if _, err := clientConn.Write(buf[:n]); err != nil {
			return fmt.Errorf("failed to write to client: %w", err)
		}
	}
}

func buildPasswordMessage(password string) []byte {
	// Формат: 'p' + длина (4 байта) + пароль + \0
	msg := make([]byte, 5+len(password)+1)
	msg[0] = 'p'
	length := 4 + len(password) + 1
	msg[1] = byte(length >> 24)
	msg[2] = byte(length >> 16)
	msg[3] = byte(length >> 8)
	msg[4] = byte(length)
	copy(msg[5:], password)
	msg[5+len(password)] = 0
	return msg
}

func (p *ProxyServer) proxyData(dst io.Writer, src io.Reader) {
	buf := make([]byte, 8192)

	for {
		n, err := src.Read(buf)
		if err != nil {
			if err != io.EOF {
				log.Printf("Proxy read error: %v", err)
			}
			return
		}

		if n > 0 {
			_, err = dst.Write(buf[:n])
			if err != nil {
				log.Printf("Proxy write error: %v", err)
				return
			}
		}
	}
}

func (p *ProxyServer) selectDatabase(dbName string) *DatabaseConfig {
	if dbName != "" {
		for i := range p.config.Databases {
			if strings.Contains(p.config.Databases[i].URL, dbName) {
				return &p.config.Databases[i]
			}
		}
	}

	if len(p.config.Databases) > 0 {
		return &p.config.Databases[0]
	}

	return nil
}

func (p *ProxyServer) sendErrorResponse(conn net.Conn, msg string) {
	log.Printf("Sending error to client: %s", msg)
	if err := conn.Close(); err != nil {
		log.Printf("Failed to close connection: %v", err)
	}
}

func extractHostPort(dsn string) string {
	dsn = strings.TrimPrefix(dsn, "postgres://")

	parts := strings.SplitN(dsn, "@", 2)
	if len(parts) == 2 {
		dsn = parts[1]
	}

	parts = strings.SplitN(dsn, "/", 2)
	if len(parts) == 2 {
		dsn = parts[0]
	}

	return dsn
}

func extractDBName(url string) string {
	url = strings.TrimPrefix(url, "jdbc:postgresql://")
	url = strings.TrimPrefix(url, "postgres://")

	parts := strings.SplitN(url, "@", 2)
	if len(parts) == 2 {
		url = parts[1]
	}

	parts = strings.SplitN(url, "/", 2)
	if len(parts) == 2 {
		dbName := strings.SplitN(parts[1], "?", 2)[0]
		return dbName
	}

	return ""
}
