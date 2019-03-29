package rocketmq

import (
	"fmt"
	"syscall"
	"testing"
)

func Test(t *testing.T) {

	for i := 0; i < 100000; i++ {
		fmt.Println(CreateUniqID())
	}

	s:= "a:"
	pid := syscall.Getpid()
	fmt.Println(s+" Getpid", pid)
	fmt.Println(s+" Getppid", syscall.Getppid())

	pgid, _ := syscall.Getpgid(pid)
	fmt.Println(s+" Getpgid", pgid)
	//fmt.Println(s+" Gettid", .Gettid())

	sid, _ := syscall.Getsid(pid)
	fmt.Println(s+" Getsid", sid)

	fmt.Println(s+" Getegid", syscall.Getegid())
	fmt.Println(s+" Geteuid", syscall.Geteuid())
	fmt.Println(s+" Getgid", syscall.Getgid())
	fmt.Println(s+" Getuid", syscall.Getuid())

}
