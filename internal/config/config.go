package config

type Config struct {
	NodeID   string        `yaml:"node_id"`
	HTTPAddr string        `yaml:"http_addr"`
	RaftAddr string        `yaml:"raft_addr"`
	Peers    []string      `yaml:"peers"`
	Storage  StorageConfig `yaml:"storage"`
}

type StorageConfig struct {
	Engine string `yaml:"engine"` // Engine: "memory"/"disk"
	Path   string `yaml:"path"`
}
