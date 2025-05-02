package main

import (
	"bufio"
	"fmt"
	"os"
	"pipline/CircBuffer"
	"strconv"
	"strings"
	"time"
)

var jobIsDone chan bool

const bufferClearInterval time.Duration = 10 * time.Second
const bufferSize int = 10

// Стадия конвейера, осуществляющая
// фильттрацию отрицательных чисел
func filterNegative(input <-chan int) <-chan int {
	filteredStream := make(chan int)
	go func() {
		defer close(filteredStream)
		for {
			select {
			case <-jobIsDone:
				return
			case i, isChannelOpen := <-input:
				if !isChannelOpen || i < 0 {
					continue //TODO:Вопрос к ментору. Если тут поставить return вместо continue, то после нескольких "некорректных чисел" будет заблокирован ввод
				}

				select {
				case filteredStream <- i:
				case <-jobIsDone:
					return
				}
			}
		}

	}()
	return filteredStream
}

// Стадия конвейера, осуществляющая
// фильттрацию чисел, не кратных 3
func filterTriple(input <-chan int) <-chan int {
	filteredStream := make(chan int)
	go func() {
		defer close(filteredStream)
		for {
			select {
			case <-jobIsDone:
				return
			case i, isChannelOpen := <-input:
				if !isChannelOpen || checkIfNotThreeDivisible(i) {
					continue //TODO:Вопрос к ментору. Если тут поставить return вместо continue, то после нескольких "некорректных чисел" будет заблокирован ввод
				}

				select {
				case filteredStream <- i:
				case <-jobIsDone:
					return
				}
			}
		}

	}()
	return filteredStream
}

func checkIfNotThreeDivisible(number int) bool {
	if number == 0 {
		return true
	}
	if number%3 == 0 {
		return false
	}
	return true
}

// Стадия конвеера, осуществляющая буферизацию
func buffering(input <-chan int) <-chan int {
	bufferedChan := make(chan int)
	cBuffer := CircBuffer.NewCircBuff(bufferSize, true)

	go func() {
		for {
			select {
			case num, isChannelOpen := <-input:
				if !isChannelOpen {
					return
				}
				cBuffer.Write(num)
			case <-jobIsDone:
				return
			}
		}
	}()

	go func() {
		for {
			select {
			case <-time.After(bufferClearInterval):
				bData, err := cBuffer.ReadAll()
				if err == nil {
					for _, elem := range bData {
						select {
						case bufferedChan <- elem:
						case <-jobIsDone:
							return
						}
					}
				}
			case <-jobIsDone:
				return
			}
		}
	}()

	return bufferedChan
}

func dataSource() <-chan int {
	source := make(chan int)
	jobIsDone = make(chan bool)
	go func() {
		defer close(jobIsDone)
		defer close(source)
		scanner := bufio.NewScanner(os.Stdin)
		var data string
		for {
			scanner.Scan()
			data = scanner.Text()
			if strings.EqualFold(data, "exit") {
				fmt.Println("До скорых встреч!")
				return
			}
			i, err := strconv.Atoi(data)
			if err != nil {
				fmt.Println("Программа принимает только целые числа!")
				continue
			}
			source <- i
		}
	}()
	return source
}

func exec(source <-chan int) {
	for {
		select {
		case data := <-source:
			fmt.Printf("Обработаны данные: %d\n", data)
		case <-jobIsDone:
			return
		}
	}
}

func pipeLine(source <-chan int) <-chan int {
	return buffering(filterTriple(filterNegative(source)))
}

func main() {
	source := dataSource()
	exec(pipeLine(source))
}
