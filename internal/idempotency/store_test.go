package idempotency

import "testing"

func TestKey_Prefix(t *testing.T) {
	got := key("01JXR4P5K2W7ZBN8M3VQ7T2YHA")
	want := "cdc:applied:01JXR4P5K2W7ZBN8M3VQ7T2YHA"
	if got != want {
		t.Fatalf("key() = %q; want %q", got, want)
	}
}

func TestKey_Empty(t *testing.T) {
	if key("") != keyPrefix {
		t.Fatal("expected lone prefix for empty event id")
	}
}
