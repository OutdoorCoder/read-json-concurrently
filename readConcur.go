package concur

import (
	"bufio"
	"bytes"
	"fmt"
	"log"
	"os"
	"runtime"
	"sync"
)

type tempStruct struct {
	field int
}

type scanByteCounter struct {
	BytesRead int64
}

type fileBufferSpecs struct {
	StartingOffset int64
	BufferSize     int64
}

func ReadFileConcur(filePath string, memorySize uint64, goRoutineCount int, killChan chan os.Signal, outPutChan chan []byte) { //, shutDownChan chan os.Signal

	//check to make sure file is valid.
	file, err := os.Open(filePath)
	if err != nil {
		fmt.Println("Error opening file: ")
		fmt.Println(err)
		//return
	}
	defer file.Close()

	//Check to see if memory size is plausabile
	var m runtime.MemStats
	runtime.ReadMemStats(&m)

	//should probably add a memory buffer for other things as well.
	//totalmemory - memory allocated = memory available < memory allowed for file reading
	//I'm pretty sure this is all in bytes so should be fine.
	if (m.Sys - m.TotalAlloc) < memorySize {
		//not enough memory available. Maybe...
		return
	} else {
		fmt.Printf("Available Mem: %d", m.Sys-m.TotalAlloc)
	}

	//check go routine count
	if goRoutineCount < 1 {
		fmt.Println("ERROR: Not enough go routines")
		return
	}

	//if a shutdown channel is present then we should pass that onto the workers.
	if killChan != nil {
		//do something here maybe
	}

	fi, err := file.Stat()
	if err != nil {
		fmt.Println("ERROR: Error getting filesize")
		return
	}
	fmt.Printf("File Size: %d", fi.Size())

	//set this to be memorySize if you want a user constraint.
	workerMem := fi.Size() / int64(goRoutineCount)
	fmt.Printf("Worker memory: %d\n", workerMem)

	//initalize file reader specs with a go routine
	startingPointChan := make(chan fileBufferSpecs)
	var wg1 sync.WaitGroup
	wg1.Add(1)
	go fileStartPointWorker(startingPointChan, file, int64(workerMem), fi.Size(), &wg1)

	var wg2 sync.WaitGroup

	readWorkerChan := createWorkQueue(goRoutineCount)

	//Copy the pattern from pasttime to do this (createworkerqueue), might take some changing up
	for buffSpec := range startingPointChan {

		<-readWorkerChan
		wg2.Add(1)
		go fileReadWorker(buffSpec, outPutChan, file, readWorkerChan, &wg2)

	}

	wg2.Wait()

	close(outPutChan)
	fmt.Println("RETURNING BYTE CHANNEL")

}

//	reate channel-based worker queue
func createWorkQueue(max int) chan bool {
	workChan := make(chan bool, max)
	for i := 1; i <= max; i++ {
		workChan <- true
	}
	return workChan
}

//Need to adjust to run within a specified amount of memory. mayb...
//buffer needs to increase in size if we can't find the bracket in the last 1k
//the 1k also needs to be a parameter or a constant
//The buffertail for loop shouldn't be a loop, it should exit on the first hit.
func fileStartPointWorker(startingPointPool chan fileBufferSpecs, inputFile *os.File, buffSizeEstimate int64, fileSize int64, wg *sync.WaitGroup) {

	defer wg.Done()

	buffTailSize := int64(1000)

	bufferTail := make([]byte, buffTailSize)
	lastBrackIndex := int64(0)
	var bufferSize int64 = 0

	for byteOffset := int64(0); byteOffset < fileSize; byteOffset += bufferSize {
		if byteOffset+buffSizeEstimate > fileSize {
			startingPointPool <- fileBufferSpecs{byteOffset, fileSize - byteOffset}
			continue
		}

		_, err := inputFile.ReadAt(bufferTail, byteOffset+buffSizeEstimate-buffTailSize)
		if err != nil {
			fmt.Print("ERROR:")
			fmt.Println(err)
			return
		}

		//Each request must be on it's own line
		for k, v := range bufferTail {
			if rune(v) == '\n' {
				lastBrackIndex = int64(k)
				break
			}
		}

		bufferSize = buffSizeEstimate - buffTailSize + lastBrackIndex + 1
		//fmt.Println("Buffertail: " + string(bufferTail))
		// fmt.Println(lastBrackIndex)
		// fmt.Println(string(bufferTail[lastBrackIndex]))

		fmt.Printf("start: %d buffersize: %d\n", byteOffset, bufferSize)
		startingPointPool <- fileBufferSpecs{byteOffset, bufferSize}
	}
	fmt.Println("DONE INITIALIZING BUFFERS")
	close(startingPointPool)

}

func fileReadWorker(buffSpecs fileBufferSpecs, jobs chan []byte, inputFile *os.File, signalChan chan bool, wg *sync.WaitGroup) {

	defer wg.Done()

	defer func() {
		signalChan <- true
	}()

	fileBuffer := make([]byte, buffSpecs.BufferSize)
	_, err := inputFile.ReadAt(fileBuffer, buffSpecs.StartingOffset)
	if err != nil {
		fmt.Println(err)
		return
	}

	counter := scanByteCounter{}
	scanner := bufio.NewScanner(bytes.NewReader(fileBuffer))
	splitFunc := counter.wrapSplitFunc(bufio.ScanLines)
	scanner.Split(splitFunc)

	foundScanData := true
	for foundScanData {
		//fmt.Println("SCANNING")
		foundScanData = scanner.Scan()

		err := scanner.Err()

		//a false return from Scan() and no error means end of file
		if !foundScanData && err == nil {
			fmt.Println("All requests have been scanned. Waiting for workers to finish.")
			continue
		} else if err != nil {
			log.Println("[ERROR]: ", err)
			continue
		}

		// .Println(scanner.Textfmt.Println("Line Read")
		// fmt())
		thisReqBytes := scanner.Bytes()
		//fmt.Println(thisReqBytes)
		reqBytes := make([]byte, len(thisReqBytes))
		copy(reqBytes, thisReqBytes)
		jobs <- reqBytes
	}

}

func (s *scanByteCounter) wrapSplitFunc(split bufio.SplitFunc) bufio.SplitFunc {
	return func(data []byte, atEOF bool) (int, []byte, error) {
		adv, tok, err := split(data, atEOF)
		s.BytesRead += int64(adv)
		return adv, tok, err
	}
}
