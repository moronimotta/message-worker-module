package message

import "encoding/json"

type Event struct {
	Event string `json:"event"`
	Data  []byte `json:"data"`
}

func (e *Event) Marshal() ([]byte, error) {
	return json.Marshal(e)
}
