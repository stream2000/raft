/*
@Time : 2020/4/4 16:54
@Author : Minus4
*/
package mr

type mapTasks struct {
	inProgress *Set
	ready      *Set
	completed  *Set
	tasks      []string
}

func initMapTasks(files []string) *mapTasks {
	t := &mapTasks{
		inProgress: NewSet(),
		ready:      NewSet(),
		completed:  NewSet(),
		tasks:      files,
	}
	for i := range files {
		t.ready.Add(i)
	}
	return t
}

type reduceTasks struct {
	inProgress *Set
	ready      *Set
	completed  *Set
	reduceN    int
}

func initReduceTasks(reduceN int) *reduceTasks {
	t := &reduceTasks{
		inProgress: NewSet(),
		ready:      NewSet(),
		completed:  NewSet(),
		reduceN:    reduceN,
	}
	for i := 0; i < reduceN; i++ {
		t.ready.Add(i)
	}
	return t
}
