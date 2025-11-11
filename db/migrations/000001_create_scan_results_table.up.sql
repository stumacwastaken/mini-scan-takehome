CREATE TABLE IF NOT EXISTS scan_results(
    id UUID PRIMARY KEY DEFAULT uuidv7(),
    ip INET NOT NULL,
    port INT NOT NULL, 
    service TEXT NOT NULL,
    response TEXT NOT NULL,
    last_seen TIMESTAMP NOT NULL,
    CONSTRAINT ip_port_service UNIQUE(ip, port, service)
);