package main

import (
	"fmt"
	"os"
)

func saveData(topic string, partition int32, data []byte) error {
	fileName := fmt.Sprintf("%s-%d.log", topic, partition)
	fmt.Println(fileName)
	f,err := os.OpenFile(fileName, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
	if err != nil{
		return err
	}
	defer f.Close()

	n := must(f.Write(data))
	fmt.Printf("Wrote %d bytes to file\n", n)
	return nil
}
