package errorer

import "fmt"

func Listen(ch <-chan error) {
	// listen chan
	for err := range ch {
		fmt.Printf("Error: %s\n", err)
	}
}
