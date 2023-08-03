package lsm

// func (sm *SSTManager) compactLevel(level int) {
// 	tables := sm.ssTables[level]
// 	kfh := make(KeyFileHeap, len(tables))

// 	for i, t := range tables {
// 		kvp, offset, _ := readKeyValue(t.DataFile, 0)
// 		kfh[i] = KeyFile{
// 			Key:    string(kvp.key),
// 			File:   i,
// 			Offset: int(offset),
// 		}
// 	}
// 	heap.Init(&kfh)

// 	for len(kfh) > 0 {
// 		kf := heap.Pop(&kfh).(KeyFile)
// 		if kf.Offset >= tables[kf.File].FileSize {
// 			continue
// 		}

// 		kvp, newOffset, _ := readKeyValue(
// 			tables[kf.File].DataFile,
// 			int64(kf.Offset),
// 		)

// 		heap.Push(&kfh, KeyFile{
// 			Key:    string(kvp.key),
// 			File:   kf.File,
// 			Offset: int(newOffset),
// 		})
// 	}
// }
