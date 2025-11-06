//  Copyright (c) 2025 Couchbase, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// 		http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package zap

import (
	"math"
	"math/rand/v2"
	"sync"

	faiss "github.com/blevesearch/go-faiss"
)

var (
	// NumGPUs is the number of available GPU devices
	NumGPUs int
	// GPULocks is a slice of mutexes for synchronizing access to GPU resources
	// Primarily used for synchronizing calls to TransferToGPU and TransferToCPU
	GPULocks []*sync.Mutex
)

func init() {
	n, err := faiss.NumGPUs()
	if err != nil {
		NumGPUs = 0
		return
	}
	NumGPUs = n
	GPULocks = make([]*sync.Mutex, NumGPUs)
	for i := 0; i < NumGPUs; i++ {
		GPULocks[i] = &sync.Mutex{}
	}
}

const (
	// GPUIndexMinVectorsForTransfer is the minimum number of vectors
	// required to consider transferring an IVF index to GPU for training
	// Smaller indexes may not benefit from GPU acceleration
	// due to transfer overheads
	GPUIndexMinVectorsForTransfer = 30000

	// GPUTransferOverheadFactor accounts for additional
	// memory overhead involved during GPU index transfer,
	// such as data transfer costs and temporary allocations
	GPUTransferOverheadFactor = 1.4

	// SoftmaxTemperature controls the sharpness of the probability distribution
	// when selecting a GPU based on available free memory.
	// Lower values (<1) make the selection more deterministic (favoring the GPU
	// with the most free memory), while higher values (>1) make it more random
	SoftmaxTemperature = 0.5
)

type gpuInfo struct {
	id      int
	freeMem float64 // in bytes
}

// GetDeviceID returns a device ID between 0 and NumGPUs-1 (inclusive) to be used for distributing work across multiple GPUs.
// It returns -1 if no GPUs are available or an error occurs. The selection algorithm favors GPUs with more free memory,
// using a softmax-based probabilistic selection to help spread load across multiple GPUs when more than two GPUs are available.
func GetDeviceID() int {
	// simple cases
	// no GPUs available, return -1
	if NumGPUs == 0 {
		return -1
	}

	var gpus []*gpuInfo
	for i := 0; i < NumGPUs; i++ {
		freeMem, err := faiss.FreeMemory(i)
		if err != nil {
			continue
		}
		gpus = append(gpus, &gpuInfo{id: i, freeMem: float64(freeMem)})
	}

	if len(gpus) == 0 {
		// fallback if no memory info available
		// return -1 to indicate no GPUs available
		// even though NumGPUs > 0 as we couldn't
		// get memory info and thus cannot provide a
		// reliable device selection
		return -1
	}

	var maxGPU *gpuInfo
	for _, g := range gpus {
		if maxGPU == nil || g.freeMem > maxGPU.freeMem {
			maxGPU = g
		}
	}

	// if all the GPUs are full, return -1
	if maxGPU.freeMem == 0 {
		return -1
	}

	// if only two or fewer GPUs, just pick the one with the most free memory
	if len(gpus) <= 2 {
		return maxGPU.id
	}

	// more than two GPUs, do softmax weighting based on free memory
	// to probabilistically pick a GPU, favoring those with more free memory
	// this helps spread load more evenly across multiple GPUs, while still
	// favoring those with more available resources, mainly to avoid
	// always picking the same GPU when multiple GPUs have similar free memory
	expVals := make([]float64, len(gpus))
	var sumExp float64
	for i, g := range gpus {
		val := math.Exp((g.freeMem - maxGPU.freeMem) / (SoftmaxTemperature * maxGPU.freeMem))
		expVals[i] = val
		sumExp += val
	}

	// Compute cumulative distribution and sample.
	r := rand.Float64()
	cumProb := 0.0
	for i, g := range gpus {
		cumProb += expVals[i] / sumExp
		if r <= cumProb {
			return g.id
		}
	}

	// Fallback to max GPU
	return maxGPU.id
}

// TrainIndex trains the given FAISS index using the provided vectors and dimensions.
// If useGPU is true and a suitable GPU is available, training is performed on the GPU;
// otherwise, training falls back to the CPU. The function returns the trained index,
// which may be a new instance if GPU training and transfer back to CPU succeed.
func TrainIndex(index *faiss.IndexImpl, vecs []float32, dims int, useGPU bool) (*faiss.IndexImpl, error) {
	// function to train index on CPU used as fallback on GPU failures
	TrainCPU := func() (*faiss.IndexImpl, error) {
		err := index.Train(vecs)
		return index, err
	}
	// decide whether to use GPU for training
	if !useGPU || NumGPUs == 0 || len(vecs)/dims < GPUIndexMinVectorsForTransfer {
		// use CPU training
		return TrainCPU()
	}
	// attempt GPU training
	deviceID := GetDeviceID()
	if deviceID == -1 {
		// no GPUs available, fallback to CPU training
		return TrainCPU()
	}
	// lock the selected GPU for the duration of the transfer and training
	GPULocks[deviceID].Lock()
	defer GPULocks[deviceID].Unlock()
	// check if enough free memory is available
	estimatedMemNeeded := uint64(float64(len(vecs)*SizeOfFloat32) * GPUTransferOverheadFactor) // input vectors + overhead
	freeMem, err := faiss.FreeMemory(deviceID)
	if err != nil || freeMem < estimatedMemNeeded {
		// unable to get free memory info or not enough free memory,
		// fallback to CPU training
		return TrainCPU()
	}
	// transfer index to GPU
	gpuIndex, err := faiss.TransferToGPU(index, deviceID)
	if err != nil {
		// transfer failed, fallback to CPU training
		return TrainCPU()
	}
	// train on GPU
	err = gpuIndex.Train(vecs)
	if err != nil {
		// training failed, fallback to CPU training
		gpuIndex.Close()
		return TrainCPU()
	}
	// transfer back to CPU
	cpuIndex, err := faiss.TransferToCPU(gpuIndex)
	gpuIndex.Close()
	if err != nil {
		// transfer back failed, fallback to CPU training
		return TrainCPU()
	}
	// successful GPU training and transfer back to CPU
	// now free the original index and return the new trained index
	index.Close()
	return cpuIndex, nil
}
