package main

import (
	"fmt"
)

func send(ch chan int) {
	for i := 0; ; i++ {
		ch <- i

		if i == 20000 {
			close(ch)
		}
	}
}

func main() {
	ch := make(chan int)

	go send(ch)

	for val := range ch {
		fmt.Println(val)
	}

}
