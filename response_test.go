package fq

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
)

func Test_parseMultiResponse(t *testing.T) {
	type args struct {
		data []byte
	}
	tests := []struct {
		name    string
		args    args
		want    multiResponseStruct
		wantErr bool
	}{
		{
			name: "1 part",
			args: args{
				data: []byte("ok|123"),
			},
			want: multiResponseStruct{
				status: ResponseStatusSuccess,
				values: []uint64{123},
				err:    nil,
			},
		},
		{
			name: "2 parts",
			args: args{
				data: []byte("ok|123;321"),
			},
			want: multiResponseStruct{
				status: ResponseStatusSuccess,
				values: []uint64{123, 321},
				err:    nil,
			},
		},
		{
			name: "3 parts",
			args: args{
				data: []byte("ok|123;321;111"),
			},
			want: multiResponseStruct{
				status: ResponseStatusSuccess,
				values: []uint64{123, 321, 111},
				err:    nil,
			},
		},
		{
			name: "error response",
			args: args{
				data: []byte("err|some error"),
			},
			want: multiResponseStruct{
				status: ResponseStatusError,
				values: nil,
				err:    errors.New("some error"),
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := parseMultiResponse(tt.args.data)
			if tt.wantErr {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
				require.Equal(t, tt.want, got)
			}
		})
	}
}

func Test_parseRateLimitResponse(t *testing.T) {
	tests := []struct {
		name    string
		data    []byte
		want    rateLimitResponseStruct
		wantErr bool
	}{
		{
			name: "allowed",
			data: []byte("ok|1;2;3;4"),
			want: rateLimitResponseStruct{
				status: ResponseStatusSuccess,
				result: RateLimitResult{
					Allowed:    true,
					Current:    2,
					Remaining:  3,
					ResetAfter: 4,
				},
			},
		},
		{
			name: "rejected",
			data: []byte("ok|0;10;0;60"),
			want: rateLimitResponseStruct{
				status: ResponseStatusSuccess,
				result: RateLimitResult{
					Allowed:    false,
					Current:    10,
					Remaining:  0,
					ResetAfter: 60,
				},
			},
		},
		{
			name: "error response",
			data: []byte("err|invalid rate limit algorithm"),
			want: rateLimitResponseStruct{
				status: ResponseStatusError,
				err:    errors.New("invalid rate limit algorithm"),
			},
		},
		{
			name:    "invalid fields count",
			data:    []byte("ok|1;2;3"),
			wantErr: true,
		},
		{
			name:    "invalid allowed field",
			data:    []byte("ok|yes;2;3;4"),
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := parseRateLimitResponse(tt.data)
			if tt.wantErr {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
				require.Equal(t, tt.want, got)
			}
		})
	}
}

func Test_parseQuotaAcquireResponse(t *testing.T) {
	tests := []struct {
		name    string
		data    []byte
		want    quotaAcquireResponseStruct
		wantErr bool
	}{
		{
			name: "acquired",
			data: []byte("ok|1;4;4;6;60"),
			want: quotaAcquireResponseStruct{
				status: ResponseStatusSuccess,
				result: QuotaAcquireResult{
					Acquired:     true,
					Allocated:    4,
					Used:         4,
					Remaining:    6,
					ExpiresAfter: 60,
				},
			},
		},
		{
			name: "rejected",
			data: []byte("ok|0;0;4;6;0"),
			want: quotaAcquireResponseStruct{
				status: ResponseStatusSuccess,
				result: QuotaAcquireResult{
					Acquired:  false,
					Allocated: 0,
					Used:      4,
					Remaining: 6,
				},
			},
		},
		{
			name: "error response",
			data: []byte("err|quota limit mismatch"),
			want: quotaAcquireResponseStruct{
				status: ResponseStatusError,
				err:    errors.New("quota limit mismatch"),
			},
		},
		{
			name:    "invalid fields count",
			data:    []byte("ok|1;4;4;6"),
			wantErr: true,
		},
		{
			name:    "invalid acquired field",
			data:    []byte("ok|yes;4;4;6;60"),
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := parseQuotaAcquireResponse(tt.data)
			if tt.wantErr {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
				require.Equal(t, tt.want, got)
			}
		})
	}
}

func Test_parseQuotaInfoResponse(t *testing.T) {
	tests := []struct {
		name    string
		data    []byte
		want    quotaInfoResponseStruct
		wantErr bool
	}{
		{
			name: "without clients",
			data: []byte("ok|10;0;10"),
			want: quotaInfoResponseStruct{
				status: ResponseStatusSuccess,
				info: QuotaInfo{
					Limit:     10,
					Remaining: 10,
					Clients:   []QuotaClientInfo{},
				},
			},
		},
		{
			name: "with clients",
			data: []byte("ok|10;7;3;client-a;4;0;client-b;3;123"),
			want: quotaInfoResponseStruct{
				status: ResponseStatusSuccess,
				info: QuotaInfo{
					Limit:     10,
					Used:      7,
					Remaining: 3,
					Clients: []QuotaClientInfo{
						{ClientID: "client-a", Amount: 4, ExpiresAt: 0},
						{ClientID: "client-b", Amount: 3, ExpiresAt: 123},
					},
				},
			},
		},
		{
			name: "error response",
			data: []byte("err|bad quota name"),
			want: quotaInfoResponseStruct{
				status: ResponseStatusError,
				err:    errors.New("bad quota name"),
			},
		},
		{
			name:    "invalid client fields count",
			data:    []byte("ok|10;7;3;client-a;4"),
			wantErr: true,
		},
		{
			name:    "invalid limit",
			data:    []byte("ok|limit;7;3"),
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := parseQuotaInfoResponse(tt.data)
			if tt.wantErr {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
				require.Equal(t, tt.want, got)
			}
		})
	}
}

func Test_parseLimitEventResponse(t *testing.T) {
	tests := []struct {
		name    string
		data    []byte
		want    limitEventResponseStruct
		wantErr bool
	}{
		{
			name: "event",
			data: []byte("ok|tenant-user_42;60;2;58"),
			want: limitEventResponseStruct{
				status: ResponseStatusSuccess,
				event: LimitEvent{
					Key:        "tenant-user_42",
					Window:     60,
					Current:    2,
					ResetAfter: 58,
				},
			},
		},
		{
			name: "error response",
			data: []byte("err|invalid stream prefix"),
			want: limitEventResponseStruct{
				status: ResponseStatusError,
				err:    errors.New("invalid stream prefix"),
			},
		},
		{
			name:    "invalid fields count",
			data:    []byte("ok|key;60;2"),
			wantErr: true,
		},
		{
			name:    "invalid window",
			data:    []byte("ok|key;window;2;58"),
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := parseLimitEventResponse(tt.data)
			if tt.wantErr {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
				require.Equal(t, tt.want, got)
			}
		})
	}
}

func Test_parseQuotaEventResponse(t *testing.T) {
	tests := []struct {
		name    string
		data    []byte
		want    quotaEventResponseStruct
		wantErr bool
	}{
		{
			name: "acquire event",
			data: []byte("ok|acq;tenant-quota;client-a;4;4;6;123"),
			want: quotaEventResponseStruct{
				status: ResponseStatusSuccess,
				event: QuotaEvent{
					Event:     "acq",
					Name:      "tenant-quota",
					ClientID:  "client-a",
					Amount:    4,
					Used:      4,
					Remaining: 6,
					ExpiresAt: 123,
				},
			},
		},
		{
			name: "delete event",
			data: []byte("ok|del;tenant-quota;;0;0;0;0"),
			want: quotaEventResponseStruct{
				status: ResponseStatusSuccess,
				event: QuotaEvent{
					Event: "del",
					Name:  "tenant-quota",
				},
			},
		},
		{
			name: "error response",
			data: []byte("err|invalid stream prefix"),
			want: quotaEventResponseStruct{
				status: ResponseStatusError,
				err:    errors.New("invalid stream prefix"),
			},
		},
		{
			name:    "invalid fields count",
			data:    []byte("ok|acq;quota;client-a;4;4;6"),
			wantErr: true,
		},
		{
			name:    "invalid amount",
			data:    []byte("ok|acq;quota;client-a;amount;4;6;0"),
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := parseQuotaEventResponse(tt.data)
			if tt.wantErr {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
				require.Equal(t, tt.want, got)
			}
		})
	}
}
