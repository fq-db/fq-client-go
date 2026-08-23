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
