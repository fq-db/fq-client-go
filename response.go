package fq

import (
	"bytes"
	"errors"
	"fmt"
	"strconv"
)

const (
	respDelimiter      = '|'
	multiDataDelimiter = ';'
)

const (
	frameStatusOK    = "ok"
	frameStatusError = "err"
	frameStatusNext  = "nxt"
)

var (
	ErrCorruptedResponse = errors.New("corrupted response")
	ErrUnknownRespStatus = errors.New("unknown response status")
)

type ErrorCode uint16

const (
	CodeInvalidSymbol                     ErrorCode = 1000
	CodeInvalidCommand                    ErrorCode = 1001
	CodeInvalidArguments                  ErrorCode = 1002
	CodeInvalidArgumentsCount             ErrorCode = 1003
	CodeMessageSizeExceedsMaximum         ErrorCode = 1004
	CodeHandshakeRequired                 ErrorCode = 1010
	CodeUnsupportedProtocolVersion        ErrorCode = 1011
	CodeProtocolVersionAlreadyNegotiated  ErrorCode = 1012
	CodeKeyCannotBeEmpty                  ErrorCode = 2000
	CodeKeyLengthExceedsMaximum           ErrorCode = 2001
	CodeBatchIsNotNumber                  ErrorCode = 2002
	CodeInvalidBatchSize                  ErrorCode = 2003
	CodeLimitIsNotNumber                  ErrorCode = 2004
	CodeInvalidLimit                      ErrorCode = 2005
	CodeInvalidRateLimitAlgorithm         ErrorCode = 2006
	CodeInvalidScanCount                  ErrorCode = 2007
	CodeInvalidScanCursor                 ErrorCode = 2008
	CodeNotAuthenticated                  ErrorCode = 3000
	CodePermissionDenied                  ErrorCode = 3001
	CodeAuthenticationFailed              ErrorCode = 3002
	CodeTooManyAuthenticationFailures     ErrorCode = 3003
	CodeQuotaNotFound                     ErrorCode = 4000
	CodeQuotaLimitMismatch                ErrorCode = 4001
	CodeQuotaAlreadyAcquiredDifferentSize ErrorCode = 4002
	CodeQuotaIsNotEmpty                   ErrorCode = 4003
	CodeQuotaLimitBelowUsedAmount         ErrorCode = 4004
	CodeQuotaOwnershipMismatch            ErrorCode = 4005
	CodeQuotaPolicyMismatch               ErrorCode = 4006
	CodeScanIndexDisabled                 ErrorCode = 5000
	CodeInspectNotAvailable               ErrorCode = 5001
	CodeInspectReportTooLarge             ErrorCode = 5002
	CodeMaxMessageSizeTooSmallForChunk    ErrorCode = 5003
	CodeInternal                          ErrorCode = 9000
	CodeInternalConfiguration             ErrorCode = 9001
)

type ProtocolError struct {
	Code    ErrorCode
	Message string
}

func (e *ProtocolError) Error() string {
	if e == nil {
		return ""
	}

	return fmt.Sprintf("%04d: %s", e.Code, e.Message)
}

func parseError(data []byte) error {
	fields := bytes.SplitN(data, []byte{respDelimiter}, 2)
	if len(fields) != 2 {
		return ErrCorruptedResponse
	}

	code, err := strconv.ParseUint(string(fields[0]), 10, 16)
	if err != nil || code > 9999 {
		return ErrCorruptedResponse
	}

	return &ProtocolError{Code: ErrorCode(code), Message: string(fields[1])}
}

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

type helloResponseStruct struct {
	status         ResponseStatus
	version        uint64
	maxMessageSize int
	authRequired   bool
	role           string
	err            error
}

func isProtocolCode(err error, code ErrorCode) bool {
	var protocolErr *ProtocolError
	if !errors.As(err, &protocolErr) {
		return false
	}

	return protocolErr.Code == code
}

func parseHelloResponse(resp []byte) (helloResponseStruct, error) {
	idx := bytes.IndexByte(resp, respDelimiter)
	if idx == -1 {
		return helloResponseStruct{}, ErrCorruptedResponse
	}

	status := string(resp[:idx])
	data := resp[idx+1:]
	switch status {
	case frameStatusOK:
		fields := bytes.Split(data, []byte{multiDataDelimiter})
		if len(fields) != 4 {
			return helloResponseStruct{}, ErrCorruptedResponse
		}

		version, err := strconv.ParseUint(string(fields[0]), 10, 64)
		if err != nil {
			return helloResponseStruct{}, ErrCorruptedResponse
		}

		maxMessageSize, err := strconv.ParseUint(string(fields[1]), 10, 31)
		if err != nil || maxMessageSize == 0 {
			return helloResponseStruct{}, ErrCorruptedResponse
		}

		authRequired, err := parseBoolField(fields[2])
		if err != nil {
			return helloResponseStruct{}, ErrCorruptedResponse
		}

		return helloResponseStruct{
			status:         ResponseStatusSuccess,
			version:        version,
			maxMessageSize: int(maxMessageSize),
			authRequired:   authRequired,
			role:           string(fields[3]),
		}, nil
	case frameStatusError:
		return helloResponseStruct{status: ResponseStatusError, err: parseError(data)}, nil
	default:
		return helloResponseStruct{}, ErrUnknownRespStatus
	}
}

func parseResponse(resp []byte) (responseStruct, error) {
	idx := bytes.IndexByte(resp, respDelimiter)
	if idx == -1 {
		return responseStruct{}, ErrCorruptedResponse
	}

	status := string(resp[:idx])
	data := string(resp[idx+1:])
	switch status {
	case frameStatusOK:
		v, err := strconv.ParseUint(data, 10, 64)
		if err != nil {
			return responseStruct{}, ErrCorruptedResponse
		}

		return responseStruct{status: ResponseStatusSuccess, value: v}, nil
	case frameStatusError:
		return responseStruct{status: ResponseStatusError, err: parseError([]byte(data))}, nil
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
	case frameStatusOK:
		values, err := respDataToValues(data)
		if err != nil {
			return multiResponseStruct{}, ErrCorruptedResponse
		}

		return multiResponseStruct{status: ResponseStatusSuccess, values: values}, nil
	case frameStatusError:
		return multiResponseStruct{status: ResponseStatusError, err: parseError(data)}, nil
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
	case frameStatusOK:
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
	case frameStatusError:
		return rateLimitResponseStruct{status: ResponseStatusError, err: parseError(data)}, nil
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
	case frameStatusOK:
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
	case frameStatusError:
		return quotaAcquireResponseStruct{status: ResponseStatusError, err: parseError(data)}, nil
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
	case frameStatusOK:
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
	case frameStatusError:
		return quotaInfoResponseStruct{status: ResponseStatusError, err: parseError(data)}, nil
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
	case frameStatusOK:
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
	case frameStatusError:
		return scanResponseStruct{status: ResponseStatusError, err: parseError(data)}, nil
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
	case frameStatusOK:
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
	case frameStatusError:
		return limitEventResponseStruct{status: ResponseStatusError, err: parseError(data)}, nil
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
	case frameStatusOK:
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
	case frameStatusError:
		return quotaEventResponseStruct{status: ResponseStatusError, err: parseError(data)}, nil
	default:
		return quotaEventResponseStruct{}, ErrUnknownRespStatus
	}
}
