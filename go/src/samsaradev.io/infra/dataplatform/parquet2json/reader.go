package parquet2json

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"os/exec"

	"github.com/samsarahq/go/oops"
)

type Parquet2JsonReader struct {
	cmd     *exec.Cmd
	decoder *json.Decoder
	stderr  *bytes.Buffer
}

func Open(filename string) (*Parquet2JsonReader, error) {
	bin, err := exec.LookPath("parquet-tools")
	if err != nil {
		return nil, oops.Wrapf(err, "parquet-tools not found in $PATH")
	}

	args := []string{"cat", "--json", fmt.Sprintf("file://%s", filename)}
	cmd := exec.Command(bin, args...)
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		return nil, oops.Wrapf(err, "stdout pipe")
	}

	var stderr bytes.Buffer
	cmd.Stderr = &stderr

	if err := cmd.Start(); err != nil {
		return nil, oops.Wrapf(err, "exec: %s %v", bin, args)
	}

	decoder := json.NewDecoder(stdout)
	return &Parquet2JsonReader{
		cmd:     cmd,
		decoder: decoder,
		stderr:  &stderr,
	}, nil
}

func (r *Parquet2JsonReader) Read(iface interface{}) error {
	if err := r.decoder.Decode(iface); err != nil {
		if err == io.EOF {
			if err := r.cmd.Wait(); err != nil {
				return oops.Wrapf(err, "parquet-tools failed: %s", r.stderr.String())
			}
			return io.EOF
		}
		return oops.Wrapf(err, "decode")
	}
	return nil
}

func (r *Parquet2JsonReader) Close() error {
	if err := r.cmd.Process.Kill(); err != nil {
		return oops.Wrapf(err, "kill")
	}
	if err := r.cmd.Wait(); err != nil {
		return oops.Wrapf(err, "kill failed")
	}
	return nil
}
