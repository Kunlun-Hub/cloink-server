package debug_bundles

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"

	"github.com/gorilla/mux"
)

func TestUploadURLAndUpload(t *testing.T) {
	h := &handler{storage: newStorage(t.TempDir())}
	req := httptest.NewRequest(http.MethodGet, "https://cloink.4w.ink/debug-bundles/upload-url?id="+testNamespace, nil)
	req.Header.Set(clientHeader, clientValue)
	req.Header.Set("X-Forwarded-Proto", "https")
	recorder := httptest.NewRecorder()
	h.uploadURL(recorder, req)
	if recorder.Code != http.StatusOK {
		t.Fatalf("upload URL status %d: %s", recorder.Code, recorder.Body.String())
	}
	var response uploadURLResponse
	if err := json.Unmarshal(recorder.Body.Bytes(), &response); err != nil {
		t.Fatal(err)
	}
	parsed, err := url.Parse(response.URL)
	if err != nil {
		t.Fatal(err)
	}
	parts := strings.Split(response.Key, "/")
	uploadReq := httptest.NewRequest(http.MethodPut, parsed.String(), bytes.NewReader([]byte("zip")))
	uploadReq = muxVars(uploadReq, parts[0], parts[1])
	uploadRecorder := httptest.NewRecorder()
	h.upload(uploadRecorder, uploadReq)
	if uploadRecorder.Code != http.StatusOK {
		t.Fatalf("upload status %d: %s", uploadRecorder.Code, uploadRecorder.Body.String())
	}
	file, _, err := h.storage.open(response.Key)
	if err != nil {
		t.Fatal(err)
	}
	file.Close()
}

func TestUploadURLRejectsWrongHostNamespace(t *testing.T) {
	h := &handler{storage: newStorage(t.TempDir())}
	req := httptest.NewRequest(http.MethodGet, "https://other.example/debug-bundles/upload-url?id="+testNamespace, nil)
	req.Header.Set(clientHeader, clientValue)
	recorder := httptest.NewRecorder()
	h.uploadURL(recorder, req)
	if recorder.Code != http.StatusBadRequest {
		t.Fatalf("got status %d, want %d", recorder.Code, http.StatusBadRequest)
	}
}

func muxVars(request *http.Request, namespace, id string) *http.Request {
	return mux.SetURLVars(request, map[string]string{"namespace": namespace, "id": id})
}
