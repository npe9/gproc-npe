// Package cluster implements a set of cluster operations that define the interactions
// between a master node and an ad hoc tree of peers and worker nodes
package cluster

import (
	"os"
	"gob"
	"gproc-npe.googlecode.com/hg/worker"
)
// RUN
// read a set of arguments from stdin
// make a directory at pathbase
// if we should make the mount private
// 	unshare
// 	unmount the pathbase
// 	and private mount the pathbase
// for all the commands we get from the arg
// 	write them out
// connect the server 
// connect to the server
// make a pipe based on the server connection
// fork with the files from the connection
// close the pipe goodies
// wait for the forked process to end

func readArgs() (arg StartArgs, err os.Error) {
	d := gob.NewDecoder(os.Stdin)
	d.Decode(&arg)
	return
}

func makeProcessDirs(pathbase string) (err os.Error) {
	err = os.Mkdir(pathbase, 0700)
	if err != nil {
		return
	}
	if DoPrivateMount == true {
		unshare()
		err = unmount(pathbase)
		if err != nil {
			return
		}
		err = privatemount(pathbase)
		if err != nil {
			return
		}
	}
	return
}

func writeExecFilesToClients(dir, file string) (err os.Error) {
	out := dir + file
	// where do I get this?

	switch {
	case fi.IsDir():
		err = os.Mkdir(out, fi.Mode&0777)
		if err != nil {
			return
		}
	case fi.IsLink():
		err = os.Symlink(out, dir+fi.Name)
		if err != nil {
			return
		}
	case fi.IsRegular():
		f, err := os.Open(out, os.O_RDWR|os.O_CREAT, 0777)
		if err != nil {
			return
		}
		defer f.Close()
		n, err := io.Copy(in, f)
		if err != nil {
			return
		}
	default:
		return
	}
	return
}

// Read reads up to len(b) bytes from the File. It returns the number of bytes
// read and an Error, if any. EOF is signaled by a zero count with err set to
// EOF.

// Run takes an argument structure from standard input, creates a set of files and
// directories based on these arguments and then executes the target command
// piping from os.Stdin to a target network connection.
func Run() (err os.Error) {
	arg, err := readArgs(os.Stdin)
	if err != nil {
		return
	}

	err = makeProcessDirs("/tmp/xproc")
	if err != nil {
		return
	}
	err = writeExecFilesToClients()
	if err != nil {
		return
	}

	p, err := pipeSocket(arg.Lserver)
	if err != nil {
		return
	}

	_, err = os.ForkExec(execPath(), arg.Args, arg.Env, pathbase, p)
	if err != nil {
		return
	}
	waitForForkedProcess()
}
// Worker
// 	get a client
// 	send the worker arguments to the client
// 	get the answer worker arguments back
// 	import all the workers
// 		and start them, 
// 		each worker is a gproc process speaking through a pipe
// 		you write data to the forked process via the pipe
// 		when it is done you return
// 	each worker is a new gproc runner
// reading their responses
// 

// RunWorkers distributes work to workers based on a StartArgs list.
func RunWorkers(fam, addr string, args StartArgs) (err os.Error) {
	c, err := networker.NewClient(fam, addr, args)
	if err != nil {
		return
	}
	args, err := c.ReadArgs()
	if err != nil {
		return
	}
	for {
		// need to import and do other stuff
	}
	// 	get the answer worker arguments back
	// 	import all the workers
	// 		and start them, 
	// 		each worker is a gproc process speaking through a pipe
	// 		you write data to the forked process via the pipe
	// 		when it is done you return
	// 	each worker is a new gproc runner
	// reading their responses
	//
}

func collectFileData()  (err os.Error) {
	// the client 
	return
}

// MASTER
// make a local socket targeting the address
// start a thread that starts an mexec based on the arguments
// 	an mexec involves collecting all of the file data.
// 	
// export a tcp netchan locally
// and make slaves on all of them

// RunMaster caches the local file data for pushing to the local worker nodes
func RunMaster() (err os.Error){
	makeLocalSocket()
	collectFileData()
	w, err := worker.NewWorker("unix", "0.0.0.0:0")
	if err != nil {
		return
	}
	makeSlave()
}



// EXEC
// get a list of stuff you need to takeout
// get the library path
// and load everything in your command file and your library path
// make workers for everything in the nodelist
// also get peers
// send arguments
// for all of the files copy them to the clients
// wait for your workers to complete

// Exec gets a list of files to send to worker nodes and peer nodes, sends
// arguments to the worker, then sends the files and waits for the worker to
// complete
func  Exec() (err os.Error) {
	// EXEC
	// get a list of stuff you need to takeout
	// get the library path
	// and load everything in your command file and your library path
	// make workers for everything in the nodelist
	// also get peers
	// send arguments
	// for all of the files copy them to the clients
	// wait for your workers to complete
}



