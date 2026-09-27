package utils

import "testing"

func TestValidateUrl(t *testing.T) {
	valid := []string{
		"https://example.com/hook",
		"http://example.com:8080/hook",
		"https://8.8.8.8/dns",
	}
	for _, raw := range valid {
		if !ValidateUrl(raw) {
			t.Fatalf("expected valid: %s", raw)
		}
	}

	invalid := []string{
		"",
		"not-a-url",
		"ftp://example.com/file",
		"file:///etc/passwd",
		"http://",
		"https://",
		"http://127.0.0.1/hook",
		"http://10.0.0.5/hook",
		"http://172.16.4.9/hook",
		"http://192.168.1.1/hook",
		"http://[::1]/hook",
		"http://localhost/hook",
		"http://api.localhost/hook",
	}
	for _, raw := range invalid {
		if ValidateUrl(raw) {
			t.Fatalf("expected invalid: %s", raw)
		}
	}
}
