package config

import (
	"fmt"
	"strings"

	"go.einride.tech/spanner-aip/internal/protoloader"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/descriptorpb"
)

// CodeGenerationConfig contains config for code generation.
type CodeGenerationConfig struct {
	// Databases to generate code for.
	Databases []DatabaseConfig `yaml:"databases"`
}

// ResourceConfig contains code generation config for a resource.
type ResourceConfig struct {
	// Message contains the Go package path and message name to use for the resource.
	// Example: go.einride.tech/aip/examples/proto/gen/einride/example/freight/v1.Shipper.
	Message string `yaml:"message"`
	// Table is the name of the table used for storing the resource.
	Table string `yaml:"table"`
}

// LoadMessageDescriptor loads the protobuf descriptor for the configured message.
func (r *ResourceConfig) LoadMessageDescriptor() (protoreflect.MessageDescriptor, error) {
	i := strings.LastIndexByte(r.Message, '.')
	if i == -1 {
		return nil, fmt.Errorf("load message descriptor: invalid message format %s", r.Message)
	}
	goImportPath, messageName := r.Message[:i], r.Message[i+1:]
	files, err := protoloader.LoadFilesFromGoPackage(goImportPath)
	if err != nil {
		return nil, fmt.Errorf("load message descriptor %s: %w", r.Message, err)
	}
	var result protoreflect.MessageDescriptor
	files.RangeFiles(func(file protoreflect.FileDescriptor) bool {
		fileOptions, ok := file.Options().(*descriptorpb.FileOptions)
		if !ok {
			return true
		}
		goPackage := fileOptions.GetGoPackage()
		if i := strings.LastIndexByte(goPackage, ';'); i != -1 {
			goPackage = goPackage[:i]
		}
		if goPackage != goImportPath {
			return true
		}
		for i := 0; i < file.Messages().Len(); i++ {
			message := file.Messages().Get(i)
			if message.Name() == protoreflect.Name(messageName) {
				result = message
				return false
			}
		}
		return true
	})
	if result == nil {
		return nil, fmt.Errorf("found no descriptor for message %s", r.Message)
	}
	return result, nil
}
