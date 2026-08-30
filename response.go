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

type limitEventResponseStruct struct {
	status ResponseStatus
	event  LimitEvent
	err    error
}

type quotaAcquireResponseStruct struct {
	status ResponseStatus
	result QuotaAcquireResult
	err    error
}

type quotaInfoResponseStruct struct {
	status ResponseStatus
	info   QuotaInfo
	err    error
}

type quotaEventResponseStruct struct {
	status ResponseStatus
	event  QuotaEvent
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

func parseQuotaAcquireResponse(resp []byte) (quotaAcquireResponseStruct, error) {
	idx := bytes.IndexByte(resp, respDelimiter)
	if idx == -1 {
		return quotaAcquireResponseStruct{}, ErrCorruptedResponse
	}

	status := string(resp[:idx])
	data := resp[idx+1:]
	switch status {
	case statusOK:
		fields := bytes.Split(data, []byte{multiDataDelimiter})
		if len(fields) != 5 {
			return quotaAcquireResponseStruct{}, ErrCorruptedResponse
		}

		acquired, err := parseBoolField(fields[0])
		if err != nil {
			return quotaAcquireResponseStruct{}, ErrCorruptedResponse
		}

		allocated, err := strconv.ParseUint(string(fields[1]), 10, 64)
		if err != nil {
			return quotaAcquireResponseStruct{}, ErrCorruptedResponse
		}

		used, err := strconv.ParseUint(string(fields[2]), 10, 64)
		if err != nil {
			return quotaAcquireResponseStruct{}, ErrCorruptedResponse
		}

		remaining, err := strconv.ParseUint(string(fields[3]), 10, 64)
		if err != nil {
			return quotaAcquireResponseStruct{}, ErrCorruptedResponse
		}

		expiresAfter, err := strconv.ParseUint(string(fields[4]), 10, 32)
		if err != nil {
			return quotaAcquireResponseStruct{}, ErrCorruptedResponse
		}

		return quotaAcquireResponseStruct{
			status: ResponseStatusSuccess,
			result: QuotaAcquireResult{
				Acquired:     acquired,
				Allocated:    allocated,
				Used:         used,
				Remaining:    remaining,
				ExpiresAfter: uint32(expiresAfter),
			},
		}, nil
	case statusError:
		return quotaAcquireResponseStruct{status: ResponseStatusError, err: errors.New(string(data))}, nil
	default:
		return quotaAcquireResponseStruct{}, ErrUnknownRespStatus
	}
}

func parseQuotaInfoResponse(resp []byte) (quotaInfoResponseStruct, error) {
	idx := bytes.IndexByte(resp, respDelimiter)
	if idx == -1 {
		return quotaInfoResponseStruct{}, ErrCorruptedResponse
	}

	status := string(resp[:idx])
	data := resp[idx+1:]
	switch status {
	case statusOK:
		fields := bytes.Split(data, []byte{multiDataDelimiter})
		if len(fields) < 3 || (len(fields)-3)%3 != 0 {
			return quotaInfoResponseStruct{}, ErrCorruptedResponse
		}

		limit, err := strconv.ParseUint(string(fields[0]), 10, 64)
		if err != nil {
			return quotaInfoResponseStruct{}, ErrCorruptedResponse
		}

		used, err := strconv.ParseUint(string(fields[1]), 10, 64)
		if err != nil {
			return quotaInfoResponseStruct{}, ErrCorruptedResponse
		}

		remaining, err := strconv.ParseUint(string(fields[2]), 10, 64)
		if err != nil {
			return quotaInfoResponseStruct{}, ErrCorruptedResponse
		}

		clients := make([]QuotaClientInfo, 0, (len(fields)-3)/3)
		for i := 3; i < len(fields); i += 3 {
			amount, err := strconv.ParseUint(string(fields[i+1]), 10, 64)
			if err != nil {
				return quotaInfoResponseStruct{}, ErrCorruptedResponse
			}

			expiresAt, err := strconv.ParseUint(string(fields[i+2]), 10, 32)
			if err != nil {
				return quotaInfoResponseStruct{}, ErrCorruptedResponse
			}

			clients = append(clients, QuotaClientInfo{
				ClientID:  string(fields[i]),
				Amount:    amount,
				ExpiresAt: uint32(expiresAt),
			})
		}

		return quotaInfoResponseStruct{
			status: ResponseStatusSuccess,
			info: QuotaInfo{
				Limit:     limit,
				Used:      used,
				Remaining: remaining,
				Clients:   clients,
			},
		}, nil
	case statusError:
		return quotaInfoResponseStruct{status: ResponseStatusError, err: errors.New(string(data))}, nil
	default:
		return quotaInfoResponseStruct{}, ErrUnknownRespStatus
	}
}

type scanResponseStruct struct {
	status ResponseStatus
	result ScanResult
	err    error
}

func parseScanResponse(resp []byte) (scanResponseStruct, error) {
	idx := bytes.IndexByte(resp, respDelimiter)
	if idx == -1 {
		return scanResponseStruct{}, ErrCorruptedResponse
	}

	status := string(resp[:idx])
	data := resp[idx+1:]
	switch status {
	case statusOK:
		fields := bytes.Split(data, []byte{multiDataDelimiter})
		if len(fields) < 1 || (len(fields)-1)%2 != 0 {
			return scanResponseStruct{}, ErrCorruptedResponse
		}

		cursor := string(fields[0])

		keys := make([]ScanKey, 0, (len(fields)-1)/2)
		for i := 1; i < len(fields); i += 2 {
			window, err := strconv.ParseUint(string(fields[i+1]), 10, 32)
			if err != nil {
				return scanResponseStruct{}, ErrCorruptedResponse
			}

			keys = append(keys, ScanKey{
				Key:    string(fields[i]),
				Window: uint32(window),
			})
		}

		return scanResponseStruct{
			status: ResponseStatusSuccess,
			result: ScanResult{
				Cursor: cursor,
				Keys:   keys,
			},
		}, nil
	case statusError:
		return scanResponseStruct{status: ResponseStatusError, err: errors.New(string(data))}, nil
	default:
		return scanResponseStruct{}, ErrUnknownRespStatus
	}
}

func parseLimitEventResponse(resp []byte) (limitEventResponseStruct, error) {
	idx := bytes.IndexByte(resp, respDelimiter)
	if idx == -1 {
		return limitEventResponseStruct{}, ErrCorruptedResponse
	}

	status := string(resp[:idx])
	data := resp[idx+1:]
	switch status {
	case statusOK:
		fields := bytes.Split(data, []byte{multiDataDelimiter})
		if len(fields) != 4 {
			return limitEventResponseStruct{}, ErrCorruptedResponse
		}

		window, err := strconv.ParseUint(string(fields[1]), 10, 32)
		if err != nil {
			return limitEventResponseStruct{}, ErrCorruptedResponse
		}

		current, err := strconv.ParseUint(string(fields[2]), 10, 64)
		if err != nil {
			return limitEventResponseStruct{}, ErrCorruptedResponse
		}

		resetAfter, err := strconv.ParseUint(string(fields[3]), 10, 32)
		if err != nil {
			return limitEventResponseStruct{}, ErrCorruptedResponse
		}

		return limitEventResponseStruct{
			status: ResponseStatusSuccess,
			event: LimitEvent{
				Key:        string(fields[0]),
				Window:     uint32(window),
				Current:    current,
				ResetAfter: uint32(resetAfter),
			},
		}, nil
	case statusError:
		return limitEventResponseStruct{status: ResponseStatusError, err: errors.New(string(data))}, nil
	default:
		return limitEventResponseStruct{}, ErrUnknownRespStatus
	}
}

func parseQuotaEventResponse(resp []byte) (quotaEventResponseStruct, error) {
	idx := bytes.IndexByte(resp, respDelimiter)
	if idx == -1 {
		return quotaEventResponseStruct{}, ErrCorruptedResponse
	}

	status := string(resp[:idx])
	data := resp[idx+1:]
	switch status {
	case statusOK:
		fields := bytes.Split(data, []byte{multiDataDelimiter})
		if len(fields) != 7 {
			return quotaEventResponseStruct{}, ErrCorruptedResponse
		}

		amount, err := strconv.ParseUint(string(fields[3]), 10, 64)
		if err != nil {
			return quotaEventResponseStruct{}, ErrCorruptedResponse
		}

		used, err := strconv.ParseUint(string(fields[4]), 10, 64)
		if err != nil {
			return quotaEventResponseStruct{}, ErrCorruptedResponse
		}

		remaining, err := strconv.ParseUint(string(fields[5]), 10, 64)
		if err != nil {
			return quotaEventResponseStruct{}, ErrCorruptedResponse
		}

		expiresAt, err := strconv.ParseUint(string(fields[6]), 10, 32)
		if err != nil {
			return quotaEventResponseStruct{}, ErrCorruptedResponse
		}

		return quotaEventResponseStruct{
			status: ResponseStatusSuccess,
			event: QuotaEvent{
				Event:     string(fields[0]),
				Name:      string(fields[1]),
				ClientID:  string(fields[2]),
				Amount:    amount,
				Used:      used,
				Remaining: remaining,
				ExpiresAt: uint32(expiresAt),
			},
		}, nil
	case statusError:
		return quotaEventResponseStruct{status: ResponseStatusError, err: errors.New(string(data))}, nil
	default:
		return quotaEventResponseStruct{}, ErrUnknownRespStatus
	}
}
