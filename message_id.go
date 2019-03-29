package rocketmq

import (
	"crypto/rand"
	"encoding/binary"
	"encoding/hex"
	"os"
	"strings"
	"sync/atomic"
	"time"
)

const (
	sz = 4 + 2 + 4 + 4 + 2
)

// GUID the global unique id generator
type GUID struct {
	fixString string
	counter   int32
}

// initialize the fixString which is the hex-encoded string of data following:
// | ip address| pid     | random  |
// +-----------+---------+---------+
// |  4 bytes  | 2 bytes | 3 bytes |
// +-----------+---------+---------+
//
func (g *GUID) init() {
	bs := make([]byte, 9)

	binary.BigEndian.PutUint32(bs[2:], uint32(os.Getpid()))
	if ip, err := GetIP(); err == nil {
		if len(ip) > 4 {
			ip = ip[len(ip)-4:]
		}
		copy(bs, ip)
	} else {
		binary.BigEndian.PutUint32(bs, uint32(unixMillis(time.Now())))
	}
	if _, err := rand.Read(bs[6:]); err != nil {
		now := uint32(unixMillis(time.Now()))
		bs[6], bs[7], bs[8] = byte(now>>16), byte(now>>8), byte(now)
	}
	g.fixString = strings.ToUpper(hex.EncodeToString(bs))
}

// Create create new global unique id hex-encoded string with length 32
//
// fixString + hex-encoded(id)
//
// the id's content is following:
//
// |<- unix time ->|<- increment num ->|
// +---------------+-------------------+
// |  4 bytes      |  3 bytes          |
// +---------------+-------------------+
func (g *GUID) Create() string {
	id := uint64(time.Now().Unix())<<24 | uint64(atomic.AddInt32(&g.counter, 1))&0xffffff
	bs := make([]byte, 1+4+3)
	binary.BigEndian.PutUint64(bs, id)

	return g.fixString + strings.ToUpper(hex.EncodeToString(bs[1:])) // since, bs[0] == id>>56 == 0
}

// NewGenerator creates the guid generator
func NewGenerator() *GUID {
	g := &GUID{}
	g.init()
	return g
}

func unixMillis(t time.Time) int64 {
	return t.UnixNano() / int64(time.Millisecond)
}

var (
	guid = NewGenerator()
)

// CreateUniqID returns the global unique id
func CreateUniqID() string {
	return guid.Create()
}
