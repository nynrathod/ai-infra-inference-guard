package admission

import "testing"

func TestAcquireRelease(t *testing.T) {
	c := New(2)

	if !c.Acquire("tenant-a") {
		t.Fatal("first acquire under limit = false")
	}
	if !c.Acquire("tenant-a") {
		t.Fatal("second acquire under limit = false")
	}
	if c.Acquire("tenant-a") {
		t.Fatal("acquire at limit = true")
	}

	c.Release("tenant-a")
	if !c.Acquire("tenant-a") {
		t.Fatal("acquire after release = false")
	}
}

func TestTenantIsolation(t *testing.T) {
	c := New(1)

	if !c.Acquire("tenant-a") {
		t.Fatal("acquire tenant-a = false")
	}
	if !c.Acquire("tenant-b") {
		t.Fatal("tenant-b blocked by tenant-a")
	}
	if c.InFlight("tenant-a") != 1 || c.InFlight("tenant-b") != 1 {
		t.Fatalf("in-flight = %d/%d, want 1/1", c.InFlight("tenant-a"), c.InFlight("tenant-b"))
	}
}

func TestSetLimit(t *testing.T) {
	c := New(1)
	c.Acquire("tenant-a")

	if c.Acquire("tenant-a") {
		t.Fatal("acquire over default limit = true")
	}

	c.SetLimit("tenant-a", 2)
	if !c.Acquire("tenant-a") {
		t.Fatal("acquire after limit raise = false")
	}
	if c.Limit("tenant-a") != 2 {
		t.Fatalf("Limit = %d, want 2", c.Limit("tenant-a"))
	}
}

func TestReleaseUnknownTenant(t *testing.T) {
	c := New(1)
	c.Release("nobody")
	if c.InFlight("nobody") != 0 {
		t.Fatalf("in-flight = %d, want 0", c.InFlight("nobody"))
	}
}
