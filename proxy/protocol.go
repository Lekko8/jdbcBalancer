package proxy

import (
	"encoding/binary"
	"fmt"
	"io"
	"net"
)

const (
	SSLRequestCode    = 80877103 // 0x04D2162F
	GSSENCRequestCode = 80877104 // 0x04D21630
	ProtocolVersion30 = 196608   // 0x00030000
)

// ReadStartupPacket читает стартовый пакет с корректной обработкой SSL и GSS
func ReadStartupPacket(conn net.Conn) ([]byte, map[string]string, error) {
	lenBuf := make([]byte, 4)
	for {
		if _, err := io.ReadFull(conn, lenBuf); err != nil {
			return nil, nil, fmt.Errorf("read startup length: %w", err)
		}

		length := binary.BigEndian.Uint32(lenBuf)
		if length < 8 || length > 10000 {
			return nil, nil, fmt.Errorf("invalid startup message length: %d", length)
		}

		body := make([]byte, length-4)
		if _, err := io.ReadFull(conn, body); err != nil {
			return nil, nil, fmt.Errorf("read startup body: %w", err)
		}

		code := binary.BigEndian.Uint32(body[:4])

		switch code {
		case SSLRequestCode, GSSENCRequestCode:
			// Отклоняем SSL и GSS на уровне прокси (байт 'N')
			if _, err := conn.Write([]byte{'N'}); err != nil {
				return nil, nil, fmt.Errorf("reject ssl/gss: %w", err)
			}
			continue

		case ProtocolVersion30:
			fullPacket := append(lenBuf, body...)
			params := parseStartupParams(body[4:])
			return fullPacket, params, nil

		default:
			return nil, nil, fmt.Errorf("unsupported protocol code: %d", code)
		}
	}
}

// parseStartupParams ну, парсит параметры
func parseStartupParams(data []byte) map[string]string {
	params := make(map[string]string)
	pos := 0
	dataLen := len(data)

	for pos < dataLen {
		keyEnd := pos
		for keyEnd < dataLen && data[keyEnd] != 0 {
			keyEnd++
		}
		if keyEnd >= dataLen || keyEnd == pos {
			break
		}
		key := string(data[pos:keyEnd])
		pos = keyEnd + 1

		valEnd := pos
		for valEnd < dataLen && data[valEnd] != 0 {
			valEnd++
		}
		if valEnd >= dataLen {
			break
		}
		val := string(data[pos:valEnd])
		pos = valEnd + 1

		params[key] = val
	}
	return params
}

// buildStartupMessage формирует пакет с подменой пользователя и базы
func buildStartupMessage(params map[string]string, backendUser, backendDB string) []byte {
	buf := make([]byte, 0, 256)
	buf = binary.BigEndian.AppendUint32(buf, ProtocolVersion30)

	for k, v := range params {
		switch k {
		case "user", "database", "authentication":
			continue
		default:
			buf = append(buf, []byte(k)...)
			buf = append(buf, 0)
			buf = append(buf, []byte(v)...)
			buf = append(buf, 0)
		}
	}

	buf = append(buf, []byte("user")...)
	buf = append(buf, 0)
	buf = append(buf, []byte(backendUser)...)
	buf = append(buf, 0)

	buf = append(buf, []byte("database")...)
	buf = append(buf, 0)
	buf = append(buf, []byte(backendDB)...)
	buf = append(buf, 0)

	buf = append(buf, 0)

	totalLen := uint32(4 + len(buf))
	packet := make([]byte, 4, totalLen)
	binary.BigEndian.PutUint32(packet, totalLen)
	packet = append(packet, buf...)
	return packet
}
