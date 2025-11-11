package repository

import (
	"context"
	"net"
	"time"

	"github.com/censys/scan-takehome/pkg/ingester"
	"github.com/jackc/pgx/v5/pgxpool"
)

type PostgresRespository struct {
	// conn *pgx.Conn
	conn *pgxpool.Pool
}

func (r *PostgresRespository) UpsertMany(ctx context.Context, scans []ingester.Scan) error {
	if len(scans) == 0 {
		return nil
	}

	ips := make([]net.IP, len(scans))
	ports := make([]int, len(scans))
	timestamps := make([]time.Time, len(scans))
	services := make([]string, len(scans))
	responses := make([]string, len(scans))

	for i, scan := range scans {
		ips[i] = net.ParseIP(scan.Ip)
		timestamps[i] = scan.Time()
		services[i] = scan.Service
		ports[i] = int(scan.Port)
		responses[i] = scan.Response
	}

	// include the conflict and where timestamp check on the upsert for general safety.
	// There is no hard requirement for there to be a cache in front of the database,
	// although it would be useful.
	_, err := r.conn.Exec(ctx, `
		INSERT INTO scan_results (ip, port, service, response, last_seen)
		SELECT * FROM
			 UNNEST($1::inet[], $2::int[], $3::text[], $4::text[], $5::timestamp[])
		ON CONFLICT ON CONSTRAINT ip_port_service
		DO UPDATE SET
			ip = EXCLUDED.ip,
			port = EXCLUDED.port,
			service = EXCLUDED.service,
			response = EXCLUDED.response,
			last_seen = EXCLUDED.last_seen
		WHERE EXCLUDED.last_seen > scan_results.last_seen
	`, ips, ports, services, responses, timestamps)

	if err != nil {
		return err
	}
	return nil
}

func NewPostgresRepository(conn *pgxpool.Pool) *PostgresRespository {
	return &PostgresRespository{
		conn: conn,
	}
}
