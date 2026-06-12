package main

import (
	"fmt"

	"github.com/jeromyritchie52/Apache-ZooKeeper/watch_manager"
)

func main() {
	wm := watch_manager.NewWatchManager()

	// Add some watches
	wm.AddWatch("/path1", "watcher1")
	wm.AddWatch("/path2", "watcher2")
	wm.AddWatch("/path1", "watcher3")

	// Remove a watch
	wm.RemoveWatch("/path1", "watcher1")

	fmt.Println("Hello, Bounty Hunter!")
}
