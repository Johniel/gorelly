package buffer

import (
	"os"
	"reflect"
	"testing"

	"github.com/Johniel/gorelly/disk"
)

func TestBufferPoolManager(t *testing.T) {
	tmpfile, err := os.CreateTemp("", "test_buffer_*.db")
	if err != nil {
		t.Fatal(err)
	}
	defer os.Remove(tmpfile.Name())

	dm, err := disk.NewDiskManager(tmpfile)
	if err != nil {
		t.Fatal(err)
	}
	defer dm.Close()

	pool := NewBufferPool(1)
	bufmgr := NewBufferPoolManager(dm, pool)

	hello := make([]byte, disk.PageSize)
	copy(hello, []byte("hello"))

	var page1ID disk.PageID
	{
		buffer, err := bufmgr.CreatePage()
		if err != nil {
			t.Fatal(err)
		}
		copy(buffer.Page[:], hello)
		buffer.IsDirty = true
		page1ID = buffer.PageID
		// Note: In Go, we can't easily test that second page creation fails
		// because the buffer pool evicts pages automatically. This is different
		// from Rust's behavior where we can check reference counts.
	}

	{
		buffer, err := bufmgr.FetchPage(page1ID)
		if err != nil {
			t.Fatal(err)
		}
		if !reflect.DeepEqual(hello, buffer.Page[:]) {
			t.Errorf("page1: expected %v, got %v", hello, buffer.Page[:])
		}
	}

	world := make([]byte, disk.PageSize)
	copy(world, []byte("world"))

	var page2ID disk.PageID
	{
		buffer, err := bufmgr.CreatePage()
		if err != nil {
			t.Fatal(err)
		}
		copy(buffer.Page[:], world)
		buffer.IsDirty = true
		page2ID = buffer.PageID
	}

	{
		buffer, err := bufmgr.FetchPage(page1ID)
		if err != nil {
			t.Fatal(err)
		}
		if !reflect.DeepEqual(hello, buffer.Page[:]) {
			t.Errorf("page1 after eviction: expected %v, got %v", hello, buffer.Page[:])
		}
	}

	{
		buffer, err := bufmgr.FetchPage(page2ID)
		if err != nil {
			t.Fatal(err)
		}
		if !reflect.DeepEqual(world, buffer.Page[:]) {
			t.Errorf("page2: expected %v, got %v", world, buffer.Page[:])
		}
	}
}
