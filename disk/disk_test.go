package disk

import (
	"os"
	"reflect"
	"testing"
)

func TestDiskManager(t *testing.T) {
	tmpfile, err := os.CreateTemp("", "test_disk_*.db")
	if err != nil {
		t.Fatal(err)
	}
	defer os.Remove(tmpfile.Name())

	disk, err := NewDiskManager(tmpfile)
	if err != nil {
		t.Fatal(err)
	}

	hello := make([]byte, PageSize)
	copy(hello, []byte("hello"))
	helloPageID := disk.AllocatePage()
	if err := disk.WritePageData(helloPageID, hello); err != nil {
		t.Fatal(err)
	}

	world := make([]byte, PageSize)
	copy(world, []byte("world"))
	worldPageID := disk.AllocatePage()
	if err := disk.WritePageData(worldPageID, world); err != nil {
		t.Fatal(err)
	}

	if err := disk.Close(); err != nil {
		t.Fatal(err)
	}

	disk2, err := OpenDiskManager(tmpfile.Name())
	if err != nil {
		t.Fatal(err)
	}
	defer disk2.Close()

	buf := make([]byte, PageSize)
	if err := disk2.ReadPageData(helloPageID, buf); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(hello, buf) {
		t.Errorf("hello page: expected %v, got %v", hello, buf)
	}

	if err := disk2.ReadPageData(worldPageID, buf); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(world, buf) {
		t.Errorf("world page: expected %v, got %v", world, buf)
	}
}
