package notifications

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestEncodeTarget(t *testing.T) {
	tests := []struct {
		name   string
		target map[string]interface{}
		want   string
	}{
		{name: "omitted", target: nil, want: ""},
		{name: "empty", target: map[string]interface{}{}, want: ""},
		{
			name:   "webhook",
			target: map[string]interface{}{"url": "https://example.com/hook"},
			want:   `{"url":"https://example.com/hook"}`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := encodeTarget(tt.target)
			require.NoError(t, err)
			require.Equal(t, tt.want, got)
		})
	}
}
