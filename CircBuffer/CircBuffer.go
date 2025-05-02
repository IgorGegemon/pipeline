package CircBuffer

import (
	"errors"
	"sync"
)

type CircBuff struct {
	buff         []int
	size         int
	writeInd     int
	readInd      int
	count        int
	isRewritable bool
	mutex        sync.Mutex
}

func NewCircBuff(size int, isRewritable bool) *CircBuff {
	return &CircBuff{
		buff:         make([]int, size),
		size:         size,
		writeInd:     0,
		readInd:      0,
		count:        0,
		isRewritable: isRewritable,
	}
}

func (buff *CircBuff) Write(num int) error {
	buff.mutex.Lock()
	defer buff.mutex.Unlock()

	if buff.count == buff.size && !buff.isRewritable {
		return errors.New("буфер полон")
	}

	buff.buff[buff.writeInd] = num
	buff.writeInd = (buff.writeInd + 1) % buff.size
	buff.count++

	return nil
}

func (buff *CircBuff) ReadOne() (int, error) {
	buff.mutex.Lock()
	defer buff.mutex.Unlock()

	if buff.count == 0 {
		return 0, errors.New("буфер пуст")
	}

	num := buff.buff[buff.readInd]
	buff.count--
	buff.readInd = (buff.readInd + 1) % buff.size

	return num, nil
}

func (buff *CircBuff) ReadAll() ([]int, error) {
	buff.mutex.Lock()
	defer buff.mutex.Unlock()

	if buff.count == 0 {
		return nil, errors.New("буфер пуст")
	}

	res := make([]int, buff.count)

	for i := 0; i < buff.count; i++ {
		res[i] = buff.buff[buff.readInd]
		buff.readInd = (buff.readInd + 1) % buff.size
	}

	buff.buff = make([]int, buff.size)
	buff.count = 0
	buff.writeInd = 0
	buff.readInd = 0

	return res, nil
}
