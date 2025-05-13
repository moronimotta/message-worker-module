package message

import "encoding/json"

type Event struct {
	Event string      `json:"event"`
	Data  interface{} `json:"data"`
}

func (e *Event) Marshal() ([]byte, error) {
	return json.Marshal(e)
}
