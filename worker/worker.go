// Package worker implements a set of abstract networked workers that can be
// used to generalize typed network io between clients and workers
package worker

import (
	"os"
	"netchan"
)

// Data represents data sent from a client to a worker
type Data struct {
	node int
	data []byte
}

// A client is a work activator, distributing work to workers
type Client struct {
	achan chan StartArg
	dchan chan Data
	rchan chan Resp
}

// NewClient creates a new client which connects to a worker at addr with
// protocol fam. The initial arguments to push to the worker are included as
// well
func NewClient(fam, addr string, arg StartArg, nodeNum int) (client *Client, err os.Error) {
	imp, err := netchan.Importer(fam, addr)
	if err != nil {
		return
	}
	achan := make(chan StartArg)
	err := imp.Import("argChan", achan, netchan.Send)
	if err != nil {
		return
	}

	dchan := make(chan Data)
	err := imp.Import("dataChan", dchan, netchan.Send)
	if err != nil {
		return
	}
	rchan := make(chan Resp)
	err := imp.Import("respChan", rchan, netchan.Recv)
	if err != nil {
		return
	}
	client = &Client{achan: achan, dchan: dchan, rchan: rchan}
	return
}

// Write writes len(b) bytes to the File. It returns the number of bytes written
// and an Error, if any. Write returns a non-nil Error when n != len(b).
func (w *Client) Write(data []byte) (n int64, err os.Error) {
	dchan <- &Data{nodeid: w.nodeid, data: data}
	n = length(d.data)
	if n <= 0 {
		err = os.EOF
		return
	}
	return
}


func (w *Client) ReadFrom(r io.Reader) (n int64, err os.Error) {
	if w, ok := r.(*Worker); ok {
		for {
			d := <-dchan
			ndata := length(d.data)
			if ndata <= 0 {
				err = os.EOF
				return
			}
			n += ndata
			w.dchan <- d
		}
		return
	}
	for {
		nread, err := r.Read(w.data)
		n += ndata
		if err != nil {
			return
		}
		nwrite, err := w.Write()
		if err != nil {
			return
		}
	}
	return
}

// A Worker is a work doer, it receives work from a client、
type Worker Client


// NewWorker creates a new worker which waits for a client connection at addr
// with protocol fam.
func NewWorker(fam, addr string) (worker *Worker, err os.Error) {
	exp, err := netchan.Exporter(fam, addr)
	if err != nil {
		return
	}
	achan := make(chan StartArg)
	err := exp.Export("argChan", achan, netchan.Recv)
	if err != nil {
		return
	}
	dchan := make(chan Data)
	err := exp.Export("dataChan", dchan, netchan.Recv)
	if err != nil {
		return
	}
	rchan := make(chan Resp)
	err := exp.Export("respChan", rchan, netchan.Send)
	if err != nil {
		return
	}
	worker = &Worker{achan: achan, dchan: dchan, rchan: rchan}
	return
}

// Read reads up to len(b) bytes from the File. It returns the number of bytes
// read and an Error, if any. EOF is signaled by a zero count with err set to
// EOF.
func (w *Worker) Read(data []byte) (n int64, err os.Error) {
	d := <-dchan
	n = length(d.data)
	if n <= 0 {
		err = os.EOF
		return
	}
	data = d.data
	return
}

func (w *Worker) WriteTo(w io.Writer) (n int, err os.Error) {
	if c, ok := w.(*Client); ok {
		for {
			d := <-dchan
			ndata := length(d.data)
			if ndata <= 0 {
				err = os.EOF
				return
			}
			n += ndata
			w.dchan <- d
		}
		return
	}
	for {
		d := <-dchan
		rn = length(d.data)
		if rn <= 0 {
			err = os.EOF
			return
		}
		n, err = w.Write(d.data)
		if err != nil {
			return
		}
	}
	return
}
