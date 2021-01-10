package codegen

import (
	"bufio"
	"bytes"
	"fmt"
	"go/parser"
	"go/printer"
	"go/token"
)

// File is a code generation file.
type File struct {
	filename string
	buf      bytes.Buffer
	err      error
}

// NewFile creates a new code generation file.
func NewFile(filename string) *File {
	f := &File{filename: filename}
	f.buf.Grow(102400) // 100kiB
	return f
}

// P prints args to the generated file.
func (f *File) P(args ...interface{}) {
	for _, arg := range args {
		_, _ = fmt.Fprint(f, arg)
	}
	_, _ = fmt.Fprintln(f)
}

// Write implements io.Writer.
func (f *File) Write(p []byte) (int, error) {
	if f.err != nil {
		return 0, f.err
	}
	n, err := f.buf.Write(p)
	if err != nil {
		f.err = fmt.Errorf("write: %w", err)
	}
	return n, err // nolint: wrapcheck // false positive
}

// Content returns the formatted Go source of the file.
func (f *File) Content() (_ []byte, err error) {
	if f.err != nil {
		return nil, fmt.Errorf("content of %s: %w", f.filename, f.err)
	}
	fileSet := token.NewFileSet()
	file, err := parser.ParseFile(fileSet, f.filename, f.buf.Bytes(), parser.ParseComments)
	if err != nil {
		var src bytes.Buffer
		s := bufio.NewScanner(bytes.NewReader(f.buf.Bytes()))
		for line := 1; s.Scan(); line++ {
			if _, err := fmt.Fprintf(&src, "%5d\t%s\n", line, s.Bytes()); err != nil {
				return nil, fmt.Errorf("content of %s: %w", f.filename, err)
			}
		}
		return nil, fmt.Errorf("content of %s:\n%v: %w", f.filename, src.String(), err)
	}
	var out bytes.Buffer
	if err := (&printer.Config{
		Mode:     printer.TabIndent | printer.UseSpaces,
		Tabwidth: 8,
	}).Fprint(&out, fileSet, file); err != nil {
		return nil, fmt.Errorf("content of %s: print source: %w", f.filename, err)
	}
	return out.Bytes(), nil
}
