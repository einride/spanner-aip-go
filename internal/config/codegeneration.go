package config

// CodeGenerationConfig contains config for code generation.
type CodeGenerationConfig struct {
	// Databases to generate code for.
	Databases []DatabaseConfig `yaml:"databases"`
}
