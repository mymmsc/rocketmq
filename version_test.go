package rocketmq

import (
	"fmt"
	"testing"
)

func TestVersion(t *testing.T) {
	fmt.Println("V4_5_0", V4_5_0)
	if V4_5_0 == 313 {
		fmt.Println("SUCCESS")
	} else {
		fmt.Println("failed")
	}
	fmt.Println(str_version[V4_5_0])
}
