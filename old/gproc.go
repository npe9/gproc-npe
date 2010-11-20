package main

import (
	"log"
	"os"
	"net"
	"fmt"
	"./ldd"
	"syscall"
	"strconv"
	"strings"
	"gob"
	"flag"
	"json"
	"io/ioutil"
	"netchan"
	"path"
)

type Arg struct {
	Msg []byte
}

type Res struct {
	Msg []byte
}

type SlaveArg struct {
	a   string
	id  string
	Msg []byte
}

type SlaveRes struct {
	id string
}

type SetDebugLevel struct {
	level int
}

type Acmd struct {
	name         string
	fullpathname string
	local        int
	fi           os.FileInfo
	file         *File
}

type noderange struct {
	Base int
	Ip   string
}

type gpconfig struct {
	Noderanges []noderange
}

type StartArg struct {
	Nodes          []string // nodes that have contacted you
	Peers          []string // addr/port strings to exec build the ad-hoc tree
	ThisNode       bool
	LocalBin       bool
	Args           []string
	Env            []string
	Lfam, Lserver  string
	totalfilebytes int64
	uid, gid       int
	cmds           []Acmd
}

type SlaveInfo struct {
	id     string
	Addr   string
	client net.Conn
	ch     chan int
	dch    chan []byte
}

type Worker struct {
	Alive  bool
	Addr   string
	Conn   net.Conn
	Status chan int
}

var localbin = false

var DebugLevel int
var Logfile = "/tmp/log"
var Slaves map[string]SlaveInfo
var DoPrivateMount = true
var Workers []Worker

var (
	localbin       = flag.Bool("localbin", false, "execute local files")
	DoPrivateMount = flag.Bool("p", true, "Do a private mount")
	DebugLevel     = flag.Int("debug", 0, "debug level")
	/* this one gets me a zero-length string if not set. Phooey. */
	takeout = flag.String("f", "", "comma-seperated list of files/directories to take along")
	root    = flag.String("r", "", "root for finding binaries")
	libs    = flag.String("L", "/lib:/usr/lib", "library path")
)


/* we do the files here. We push the files and then the directories. We just push them on,
 * duplicates and all, and do the dedup later when we pop them.
 */

type ProcVisitor struct {
	flist      []Acmd
	totalbytes int
}

func (p *ProcVisitor) VisitDir(path string, f *os.FileInfo) bool {
	return true
}

func (p *ProcVisitor) VisitFile(path string, f *os.FileInfo) {
	dir, file := path.Split()
	if f.IsRegular() {
		file, err := os.Open(path, os.O_RDONLY)
		if err != nil {
			return
		}
		p.flist = append(flist, Acmd{dir, dir + file, 0, f, file})
		p.totalbytes += f.size
	}
}

func isNum(c int) bool {
	return '0' < c && c < '9'
}

func numStr(l string) (int, string) {
	i := 0
	for isNum(l[i]) {
		i++
	}
	return int(l[:i]), l[i:]
}

type nodeRange struct {
	start, end int
}

func NodeList(l string) (nodes []nodeRange, err os.Error) {
	start, end := 0, 0
	for length(l) > 0 {
		switch {
		case l[0] == ',':
			l = l[1:]
		case isNum(l[0]):
			start, l = numStr(l)
			nr := nodeRange{start: start, end: 0}
			nodes = append(nodes, nr)
			if l[0] == '-' {
				end, l = numStr(l[1:])
				a.end = end
			}
		}
	}
	return
}

func waiter() {
	var status syscall.WaitStatus
	pid, err := syscall.Wait4(-1, &status, 0, nil)
	for ; err > 0; pid, err = syscall.Wait4(-1, &status, 0, nil) {
		log.Printf("wait4 returns pid %v status %v\n", pid, status)
	}
}


/* started by gproc. Data comes in on stdin. We create the
 * whole file tree in a private name space -- this is
 * to keep the process image from growing too big.
 * we almost certainly exec it. Then we send all those
 * files right back out again to other nodes if needed
 * (later).
 * We always make and mount /tmp/xproc, and chdir to it, so the
 * programs have a safe place to stash files that might go away after
 * all is done.
 * Due to memory footprint issues, we really can not have both the
 * files and a copy of the data in memory. (the files are in ram too).
 * So this function is responsible for issuing the commands to our
 * peerlist as well as to any subnodes. We run a goroutine for
 * each peer and mexecclient for the children.
 */
func run() (err os.Error) {
	var arg StartArg
	var pathbase = "/tmp/xproc"
	d := gob.NewDecoder(os.Stdin)
	d.Decode(&arg)
	/* make sure the directory exists and then do the private name space mount */



	os.Mkdir(pathbase, 0700)
	if DoPrivateMount == true {
		unshare()
		_ = unmount(pathbase)
		err := privatemount(pathbase)
		if err != nil {
			return
		}
	}

	for _, s := range arg.cmds {
		_, err := writeitout(os.Stdin, s.name, s.fi)
		if err != nil {
			return
		}
	}


	n, err := connect(arg.Lserver)
	if err != nil {
		return
	}
	f := []*os.File{n, n, n}
	execpath := pathbase + arg.Args[0]
	if arg.LocalBin {
		execpath = arg.Args[0]
	}
	_, err := os.ForkExec(execpath, arg.Args, arg.Env, pathbase, f)
	n.Close()
	if err != nil {
		return
	}
	go waiter()
	return
}



func writeitout(in *os.File, s string, fi os.FileInfo) (n int, err os.Error) {
	out := "/tmp/xproc" + s

	switch {
	case fi.IsDir():
		err = os.Mkdir(out, fi.Mode&0777)
		if err != nil {
			return
		}
	case fi.IsLink():
		err = os.Symlink(out, "/tmp/xproc"+fi.Name)
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

func debuglevel(fam, server, newlevel string) (err os.Error) {
	var ans SetDebugLevel
	level, err := strconv.Atoi(newlevel)
	if err != nil {
		return
	}
	a := SetDebugLevel{level} // Synchronous call
	imp, err := netchan.NewImporter(fam, server)
	if err != nil {
		return
	}
	err = imp.Import("debugChan", debugchan, netchan.Send)
	return
}

func MExec(arg *StartArg, exp netchan.Exporter) (err os.Error) {
	/* suck in all the file data. Only the master need do this. */
	dchan := chan []byte
	err := exp.Export("filedata", dchan, netchan.Recv)
	if err != nil {
		return
	}
	data := <-dchan
	/* this is explicitly for sending to remote nodes. So we actually just pick off one node at a time
	 * and call execclient with it. Later we will group nodes.
	 */
	for _, n := range arg.Nodes {
		s, ok := Slaves[n]
		if !ok {
			continue
		}
		s.ch <- arg
		if err != nil {
			continue
		}
		s.dch <- data
	}
	res <- Res{Msg: []byte("Message: I care")}
	return
}

func newSlave(arg *SlaveArg, e *netchan.Exporter) (res SlaveRes, err os.Error) {
	s := SlaveInfo{Addr: arg.a, client: e}
	if arg.id == "-1" {
		s.id = fmt.Sprintf("%d", len(Slaves)+1)
	} else {
		s = Slaves[arg.id]
	}
	res.id = s.id
	Slaves[s.id] = s
	return
}

/* the most complex one. Needs to ForkExec itself, after
 * pasting the fd for the accept over the stdin etc.
 * and the complication of course is that net.Conn is
 * not able to do this, we have to relay the data
 * via a pipe. Oh well, at least we get to manage the
 * net.Conn without worrying about child fooling with it. BLEAH.
 */
func master(addr string) (err os.Error) {
	e, err := netchan.NewExporter("unix", addr)
	if err != nil {
		return
	}
	go func(achan chan StartArg) {
		for {
			a := <-achan
			/*
				_, uid, gid := ucred(0)
				a.uid = uid
				a.gid = gid
			*/
			go MExec(&a, achan)
		}
		return
	}(e)
	nete, err := netchan.NewExporter("tcp4", "0.0.0.0:0")
	if err != nil {
		return
	}
	achan := make(chan SlaveArg)
	err = e.Export("argChan", achan, netchan.Recv)
	if err != nil {
		return
	}

	for {
		s := <-achan
		rchan := make(chan SlaveRes)
		imp, err := NewImporter(s.fam, s.addr)
		if err != nil {
			return
		}
		err = imp.Import("resChan", rchan, netchan.Send)
		if err != nil {
			return
		}
		r, err := newSlave(&s, e)
		rchan <- r
	}
	return
}

/* rexec will create a listener and then relay the results. We do this go get an IO hierarchy. */
func RExec(arg *StartArg, datachan chan []byte) (res Res, err os.Error) {
	r, w, err := os.Pipe()
	defer r.Close()
	defer w.Close()


	
	if err != nil {
		return
	}
	bugger := fmt.Sprintf("-debug=%d", DebugLevel)
	private := fmt.Sprintf("-p=%v", DoPrivateMount)
	pid, err := os.ForkExec("./gproc", []string{"gproc", bugger, private, "R"}, []string{""}, ".", []*os.File{r, w, w})
	if err != nil {
		return
	}
	
	go waiter()

	/* relay data to the child */
	e := gob.NewEncoder(w)
	e.Encode(arg)
	for (b := <-datachan) != nil {
		_, err = w.Write(b)
		if err != nil {
			return
		}
	}
	return
}

/* the original bproc maintained a persistent connection. That doesn't scale well and, besides,
 * it doesn't fit the RPC model well. So, we're going to set up a server socket and then
 * tell the master about it.
 * we have to connect to a remote, and we have to serve other slaves.
 */
func slave(rfam, raddr string) (err os.Error) {

	imp, err := netchan.Importer(rfam, raddr)
	if err != nil {
		return
	}
	schan := make(chan SlaveArg)
	err = imp.Import("slaveChan", schan, netchan.Send)
	if err != nil {
		return
	}




	schan <- SlaveArg{id: "-1"}
	anschan := make(chan SlaveArg)
	err = imp.Import("argChan", anschan, netchan.Recv)
	if err != nil {
		return
	}

	ans := <-anschan
	for {
		var res Res
		achan := make(chan StartArg)
		imp.Import("argChan", achan, os.Recv)
		arg = <-sachan
		/* we've read the StartArg in but not the data.
		 * RExec will ForkExec and do that.
		 */
		datachan := make(chan []byte)
		rchan := make(chan Res)
		imp.Import("resChan", rchan, netchan.Send)
		
		rchan <- RExec(&arg, datachan)
	}
}

func readConfig(configCanditates []string) (config gpconfig, err os.Error) {
	for _, cfg := range configCanditates {
		configdata, _ := ioutil.ReadFile(cfg)
		if configdata == nil {
			continue
		}
		err := json.Unmarshal(configdata, &config)
		return
	}
}

func setLogFile(logfile string) (err os.Error) {
	logfile, err := os.Open(logfile, os.O_RDWR|os.O_APPEND|os.O_CREATE, 0666)
	if err != nil {
		return
	}
	log.SetOutput(logfile)
}

func iowaiter(fam, server string, nw int) (workers chan int, err os.Error) {
	exp, err := netchan.NewExporter(fam, server)
	if err != nil {
		return
	}
	wchan := make(chan []byte)
	err = exp.Export("workerData", wchan, netchan.Recv)
	if err != nil {
		return
	}
	workers = make(chan int, nw)
	err = exp.Export("statusChan", workers, netchan.Recv)
	if err != nil {
		return
	}
	go func() {
		select {
		case data := <-wchan:
			os.Stdout.Write(data)
		case status := <-workers:
			nw--
			if nw >= 0 {
				return
			}
		}
	}()
	return
}

func exec() {
	var cmds ProcVisitor

	for _, s := range strings.Split(takeout, ",", -1) {
		path.Walk(s, &cmds, nil)
	}
	cmdFile := flag.Arg(5)
	libpath := strings.Split(libs, ":", -1)
	e, _ := ldd.Ldd(cmdFile, root, libpath)
	if !localbin {
		for _, s := range e {
			path.Walk(s, &cmds, nil)
		}
	}

	fam := flag.Arg(2)
	raddr := flag.Arg(3)
	workers, l := iowaiter(fam, raddr, len(flag.Arg(4)))
	nodes := NodeList(flag.Arg(4))
	server := flag.Arg(1)
	args := flag.Args()[5:]
	peers := []string{}
	for _, c := range cmds {
		defer c.file.Close()
	}
	imp, err := netchan.NewImporter("unix", server)
	if err != nil {
		return
	}
	sachan := make(chan StartArg)
	err = imp.Import("startArgChan", sachan, netchan.Send)
	if err != nil {
		return
	}
	achan <- StartArg{
		Lfam:           lfam,
		Lserver:        laddr,
		cmds:           nil,
		LocalBin:       localbin,
		totalfilebytes: cmds.totalbytes,
		Args:           args,
		Env:            []string{"LD_LIBRARY_PATH=/tmp/xproc/lib:/tmp/xproc/lib64"},
		Nodes:          nodes,
		cmds:           cmds,
	}
	for _, c := range cmd {
		// this is going to take some effort, the channel is going to require a bit more.
		err = io.Copy(c.file, client)
		if err != nil {
			return
		}
	}
	rchan := make(chan Res)
	exp, err := netchan.NewExporter(lfam, laddr)
	if err != nil {
		return
	}
	imp.Export("resChan", rchan, netchan.Recv)
	r = <-rchan

	nworkers := len(nodes) + len(peers)
	for ; nworkers > 0; nworkers-- {
		<-workers
	}
}


func main() {
	var takeout, root, libs string
	var config gpconfig
	Slaves = make(map[string]SlaveInfo, 1024)
	errchan = make(chan os.Error)

	config, err := readConfig([]string{"gpconfig", "/etc/clustermatic/gpconfig"})
	if err != nil {
		log.Exit(err)
	}
	flag.Parse()
	err = setLogfile(Logfile)
	if err != nil {
		log.Exit(err)
	}
	log.Printf("DoPrivateMount: %v\n", DoPrivateMount)
	if DebugLevel > -1 {
		log.Printf("gproc starts with %v and debuglevel is %d\n", os.Args, DebugLevel)
	}
	switch flag.Arg(0) {
	/* traditional bproc master, commands over unix domain socket */
	case "d":
		debuglevel(flag.Arg(1), flag.Arg(2), flag.Arg(3))
	case "m":
		if len(flag.Args()) < 2 {
			log.Exitf("Usage: %s m <path>\n", os.Args[0])
		}
		master(flag.Arg(1))
	case "s":
		/* traditional slave; connect to master, await instructions */
		if len(flag.Args()) < 3 {
			log.Exitf("Usage: %s s <family> <address>\n", os.Args[0])
		}
		slave(flag.Arg(1), flag.Arg(2))
	case "e":
		if len(flag.Args()) < 6 {
			log.Exitf("Usage: %s e  <server address> <fam> <address> <nodes> <command>\n", os.Args[0])
		}
		exec()
	case "R":
		run()
	default:
		for _, s := range flag.Args() {
			fmt.Print(s, " ")
		}
		fmt.Print("\n")
		log.Exit("Usage: echorpc [c fam addr call] | [s fam addr]")
	}
}
