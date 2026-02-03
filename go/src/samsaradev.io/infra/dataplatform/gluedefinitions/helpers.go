package gluedefinitions

type ColumnStruct struct {
	Name     string      `json:"name"`
	Type     string      `json:"type"`
	Nullable bool        `json:"nullable"`
	Metadata interface{} `json:"metadata"`
}

type TableStruct struct {
	Type   string         `json:"type"`
	Fields []ColumnStruct `json:"fields"`
}
