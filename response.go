package fq

import (
	"bytes"
	"errors"
	"strconv"
)

const (
	respDelimiter      = '|'
	multiDataDelimiter = ';'
)

var (
	ErrCorruptedResponse = errors.New("corrupted response")
	ErrUnknownRespStatus = errors.New("unknown response status")
)

var (
	statusOK    = "ok"
	statusError = "err"
)

type ResponseStatus uint8

const (
	ResponseStatusUnknown ResponseStatus = iota
	ResponseStatusSuccess
	ResponseStatusError
)

type responseStruct struct {
	status ResponseStatus
	value  uint64
	err    error
}

func parseResponse(resp []byte) (responseStruct, error) {
	idx := bytes.IndexByte(resp, respDelimiter)
	if idx == -1 {
		return responseStruct{}, ErrCorruptedResponse
	}

	status := string(resp[:idx])
	data := string(resp[idx+1:])
	switch status {
	case statusOK:
		v, err := strconv.ParseUint(data, 10, 64)
		if err != nil {
			return responseStruct{}, ErrCorruptedResponse
		}

		return responseStruct{status: ResponseStatusSuccess, value: v}, nil
	case statusError:
		return responseStruct{status: ResponseStatusError, err: errors.New(data)}, nil
	default:
		return responseStruct{}, ErrUnknownRespStatus
	}
}

type multiResponseStruct struct {
	status ResponseStatus
	values []uint64
	err    error
}

type rateLimitResponseStruct struct {
	status ResponseStatus
	result RateLimitResult
	err    error
}

func parseMultiResponse(resp []byte) (multiResponseStruct, error) {
	idx := bytes.IndexByte(resp, respDelimiter)
	if idx == -1 {
		return multiResponseStruct{}, ErrCorruptedResponse
	}

	status := string(resp[:idx])
	data := resp[idx+1:]
	switch status {
	case statusOK:
		values, err := respDataToValues(data)
		if err != nil {
			return multiResponseStruct{}, ErrCorruptedResponse
		}

		return multiResponseStruct{status: ResponseStatusSuccess, values: values}, nil
	case statusError:
		return multiResponseStruct{status: ResponseStatusError, err: errors.New(string(data))}, nil
	default:
		return multiResponseStruct{}, ErrUnknownRespStatus
	}
}

func respDataToValues(data []byte) ([]uint64, error) {
	var values []uint64

	idx := bytes.IndexByte(data, multiDataDelimiter)
	if idx == -1 {
		v, err := strconv.ParseUint(string(data), 10, 64)
		if err != nil {
			return nil, err
		}

		return []uint64{v}, nil
	}

	for idx >= 0 {
		part := data[0:idx]
		v, err := strconv.ParseUint(string(part), 10, 64)
		if err != nil {
			return nil, err
		}

		values = append(values, v)

		if idx == len(data) {
			break
		}

		data = data[idx+1:]
		idx = bytes.IndexByte(data, multiDataDelimiter)
		if idx == -1 {
			idx = len(data)
		}
	}

	return values, nil
}

func parseRateLimitResponse(resp []byte) (rateLimitResponseStruct, error) {
	idx := bytes.IndexByte(resp, respDelimiter)
	if idx == -1 {
		return rateLimitResponseStruct{}, ErrCorruptedResponse
	}

	status := string(resp[:idx])
	data := resp[idx+1:]
	switch status {
	case statusOK:
		fields := bytes.Split(data, []byte{multiDataDelimiter})
		if len(fields) != 4 {
			return rateLimitResponseStruct{}, ErrCorruptedResponse
		}

		allowed, err := parseBoolField(fields[0])
		if err != nil {
			return rateLimitResponseStruct{}, ErrCorruptedResponse
		}

		current, err := strconv.ParseUint(string(fields[1]), 10, 64)
		if err != nil {
			return rateLimitResponseStruct{}, ErrCorruptedResponse
		}

		remaining, err := strconv.ParseUint(string(fields[2]), 10, 64)
		if err != nil {
			return rateLimitResponseStruct{}, ErrCorruptedResponse
		}

		resetAfter, err := strconv.ParseUint(string(fields[3]), 10, 32)
		if err != nil {
			return rateLimitResponseStruct{}, ErrCorruptedResponse
		}

		return rateLimitResponseStruct{
			status: ResponseStatusSuccess,
			result: RateLimitResult{
				Allowed:    allowed,
				Current:    current,
				Remaining:  remaining,
				ResetAfter: uint32(resetAfter),
			},
		}, nil
	case statusError:
		return rateLimitResponseStruct{status: ResponseStatusError, err: errors.New(string(data))}, nil
	default:
		return rateLimitResponseStruct{}, ErrUnknownRespStatus
	}
}

func parseBoolField(data []byte) (bool, error) {
	switch string(data) {
	case "0":
		return false, nil
	case "1":
		return true, nil
	default:
		return false, ErrCorruptedResponse
	}
}
