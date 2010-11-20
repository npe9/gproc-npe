package ldd

import (
	"os"
	"./elf"
	"path"
	"strings"
	"fmt"
)

// not global, sadly
// getString extracts a string from an ELF string table.
func getString(section []byte, start int) (string, bool) {
	if start < 0 || start >= len(section) {
		return "", false
	}

	for end := start; end < len(section); end++ {
		if section[end] == 0 {
			return string(section[start:end]), true
		}
	}
	return "", false
}

type FindVisitor struct {
	localname string
	remotename string
	fileInfo *os.FileInfo
}

func (p *FindVisitor) VisitDir(path string, f *os.FileInfo) bool {
	return true
}

func (p *FindVisitor) VisitFile(path string, f *os.FileInfo) {
	if path == localname {
		p.remotename = path
		p.fileInfo = f
	}

}



func dynlibs(f *elf.File) ([]string, os.Error) {
	var Libs []string
	var dynstrndx int
	dynstrndx = -1
	var err os.Error
	var dyndata []byte // the .dynamic section
	var dyndstr []byte // the dynamic strings

	for _, p := range f.Progs {
		if p.Type == elf.PT_INTERP {
			var name []byte
			var err os.Error
			if name, err = p.Data(); err != nil {
				return nil, err
			}
			Libs = make([]string, 1)
			Libs[0] = string(name)
		}
	}

	for i, s := range f.Sections {
		if s.Name == ".dynstr" {
			dynstrndx = i
		}
	}
	/* Load dynamic string table (if it exists) and then get all the lib names*/
	if dynstrndx > -1 {
		s := f.Sections[dynstrndx]
		dyndstr, err = s.Data()
		if err != nil {
			return nil, err
		}
		for _, s := range f.Sections {
			//	var name string
			if s.Type != elf.SHT_DYNAMIC {
				continue
			}
			dyndata, err = s.Data()
			if err != nil {
				return nil, err
			}
			break
		}
	}

	if dyndata == nil {
		return Libs, nil
	}
	/* walk through the dynamic section entries. Blow out when the type is null. */
	for i := 0; i < len(dyndata); {
		var t uint64
		var doff uint64
		switch f.Class {
		case elf.ELFCLASS32:
			t = uint64(int(dyndata[i+0]) + int(dyndata[i+1])<<8 + int(dyndata[i+2])<<16 + int(dyndata[i+3])<<24)
			i += 4
			doff = uint64(int(dyndata[i+0]) + int(dyndata[i+1])<<8 + int(dyndata[i+2])<<16 + int(dyndata[i+3])<<24)
			i += 4
		case elf.ELFCLASS64:
			t = uint64(int(dyndata[i+0]) + int(dyndata[i+1])<<8 + int(dyndata[i+2])<<16 + int(dyndata[i+3])<<24)
			i += 4
			t += uint64(int(dyndata[i+0]) + int(dyndata[i+1])<<8 + int(dyndata[i+2])<<16 + int(dyndata[i+3])<<24)
			i += 4
			doff = uint64(int(dyndata[i+0]) + int(dyndata[i+1])<<8 + int(dyndata[i+2])<<16 + int(dyndata[i+3])<<24)
			i += 4
			doff += uint64(int(dyndata[i+0]) + int(dyndata[i+1])<<8 + int(dyndata[i+2])<<16 + int(dyndata[i+3])<<24)
			i += 4
		}
		if elf.DynTag(t) == elf.DT_NULL {
			break
		}
		if elf.DynTag(t) != elf.DT_NEEDED {
			continue
		}
		name, _ := getString(dyndstr, int(doff))
		n := len(Libs)
		libs := make([]string, n+1)
		copy(libs, Libs)
		libs[n] = string(name)
		Libs = libs
	}
	return Libs, nil
}

/* rule: if first char in path is '/', don't apply the path. The binary should
 * have the full path.
 */
func ldd(known map[string]bool, name, root string, libpath []string) (err os.Error) {
	var p FindVisitor
	/*
	what about libpath?
	*/
	p.localname = name
	path.Walk(&p, root)
	if known[remotename] == true {
		return
	}
	e, err := elf.Open(localname)
	if err != nil {
		return
	}
	known[remotename] = true
	Libs, err := dynlibs(e)
	if err != nil {
		return
	}
	for _, s := range Libs {
		err := ldd(known, s, root, libpath)
		if err != nil {
			return
		}
	}
	return nil
}

func Ldd(cmd, root, libpath []string) (ret []string, err os.Error) {
	var libpath []string
	known := make(map[string]bool, 16)
	ldd(known, cmd, root, libpath)
	i := len(known)
	ret := make([]string, i+1)
	/* I did not do this quite right */
	ret[0] = cmd
	known[cmd] = false
	cur := 1
	for key, val := range known {
		if val == false {
			continue
		}
		ret[cur] = key
		cur++
	}
	return
}

