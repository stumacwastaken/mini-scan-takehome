package ingester

import (
	"encoding/json"
	"errors"
	"time"

	"github.com/censys/scan-takehome/pkg/scanning"
)

// private struct to take the scanning information and transform the
// "Data" from an interface to a versioned struct so we can fetch the
// response string. Could have used a map, but this feels a bit safer
// in the event a garbled data version comes in
type Scan struct {
	scanning.Scan
	Data     json.RawMessage `json:"data"`
	Response string
}

func (s *Scan) UnmarshalJSON(b []byte) error {
	var container struct {
		Ip          string          `json:"ip"`
		Port        uint32          `json:"port"`
		Service     string          `json:"service"`
		Timestamp   int64           `json:"timestamp"`
		DataVersion int             `json:"data_version"`
		Data        json.RawMessage `json:"data"`
	}

	if err := json.Unmarshal(b, &container); err != nil {
		return err
	}

	s.Ip = container.Ip
	s.Port = container.Port
	s.Service = container.Service
	s.DataVersion = container.DataVersion
	s.Timestamp = container.Timestamp

	switch s.DataVersion {
	case scanning.V1:
		var v1Data scanning.V1Data
		if err := json.Unmarshal(container.Data, &v1Data); err != nil {
			return err
		}
		s.Response = string(v1Data.ResponseBytesUtf8)
	case scanning.V2:
		var v2Data scanning.V2Data
		if err := json.Unmarshal(container.Data, &v2Data); err != nil {
			return err
		}
		s.Response = v2Data.ResponseStr
	default:
		return errors.New("unrecognized Data Version type")
	}
	return nil
}

func (s *Scan) Time() time.Time {
	return time.Unix(s.Timestamp, 0)
}
