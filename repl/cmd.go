package repl

import (
	"errors"
	"fmt"
	"strconv"
	"strings"
)

type Cmd struct {
	Cmd   string
	Named map[string]string
	Pos   []string
}

func ParseCmd(line string) Cmd {
	result := Cmd{
		Named: make(map[string]string),
	}
	parts := strings.Fields(line)
	if len(parts) > 0 {
		result.Cmd = parts[0]
		for _, part := range parts[1:] {
			named := strings.Split(part, "=")
			if len(named) > 1 {
				v, ok := result.Named[named[0]]
				if ok {
					result.Named[named[0]] = v + "," + part[len(named[0])+1:]
				} else {
					result.Named[named[0]] = part[len(named[0])+1:]
				}
			} else {
				result.Pos = append(result.Pos, part)
			}
		}
	}
	return result
}

func (c Cmd) IsEmpty() bool {
	return len(c.Cmd) == 0
}

func (c Cmd) GetUInt(i interface{}) (uint64, error) {
	var val string
	switch key := i.(type) {
	case string:
		var ok bool
		val, ok = c.Named[key]
		if !ok {
			return 0, errors.New(fmt.Sprintf("parameter %s was not defined", key))
		}
	case int:
		if key < 0 || key > len(c.Pos) {
			return 0, errors.New(fmt.Sprintf("not enough parameters. %d provided, %d reading", len(c.Pos), key))
		}
		val = c.Pos[key]
	default:
		panic("can only read indexed and named arguments")
	}
	return strconv.ParseUint(val, 10, 64)
}

func (c Cmd) GetUIntOr(i interface{}, def uint64) (uint64, error) {
	var val string
	switch key := i.(type) {
	case string:
		var ok bool
		val, ok = c.Named[key]
		if !ok {
			return def, nil
		}
	case int:
		if key < 0 || key > len(c.Pos) {
			return def, nil
		}
		val = c.Pos[key]
	default:
		panic("can only read indexed and named arguments")
	}
	return strconv.ParseUint(val, 10, 64)
}

func (c Cmd) GetUIntSliceOr(key string, def []uint64) ([]uint64, error) {
	vals, ok := c.Named[key]
	if !ok {
		return def, nil
	}
	var res []uint64
	for _, v := range strings.Split(vals, ",") {
		iv, err := strconv.ParseUint(v, 10, 64)
		if err != nil {
			return nil, err
		}
		res = append(res, iv)
	}
	return res, nil
}
