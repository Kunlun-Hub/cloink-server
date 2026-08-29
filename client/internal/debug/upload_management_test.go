package debug

import "testing"

func TestManagementBundleURL(t *testing.T) {
	tests := []struct {
		name    string
		input   string
		want    string
		wantErr bool
	}{
		{name: "production", input: "https://cloink.4w.ink:443", want: "https://cloink.4w.ink:443/api/debug-bundles/upload-url"},
		{name: "strips path and query", input: "https://example.test/management?old=1", want: "https://example.test/api/debug-bundles/upload-url"},
		{name: "rejects insecure", input: "http://example.test", wantErr: true},
		{name: "rejects relative", input: "example.test", wantErr: true},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			got, err := ManagementBundleURL(test.input)
			if test.wantErr {
				if err == nil {
					t.Fatalf("expected an error, got %q", got)
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			if got != test.want {
				t.Fatalf("got %q, want %q", got, test.want)
			}
		})
	}
}
