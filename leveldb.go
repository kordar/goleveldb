package goleveldb

import (
	"encoding/json"
	"fmt"
	"github.com/syndtr/goleveldb/leveldb"
	"github.com/syndtr/goleveldb/leveldb/errors"
	"github.com/syndtr/goleveldb/leveldb/iterator"
	"github.com/syndtr/goleveldb/leveldb/util"
)

type LevelDB struct {
	db       *leveldb.DB
	filepath string
}

func NewLevelDB(filepath string) *LevelDB {
	return &LevelDB{filepath: filepath}
}

func (level *LevelDB) GetDB() (*leveldb.DB, error) {
	if level.db != nil {
		return level.db, nil
	}

	if db, err := leveldb.OpenFile(level.filepath, nil); err != nil {
		// 是否corrupt异常，是重新创建
		if errors.IsCorrupted(err) {
			if db, err = leveldb.RecoverFile(level.filepath, nil); err == nil {
				level.db = db
				return db, nil
			}
		}
		return nil, fmt.Errorf("failed to open leveldb at %s: %s", level.filepath, err)
	} else {
		level.db = db
		return db, nil
	}
}

// Close 关闭句柄
func (level *LevelDB) Close() {
	if level.db != nil {
		_ = level.db.Close()
		level.db = nil
	}
}

// Has 判断key是否存在
func (level *LevelDB) Has(key string) (bool, error) {
	db, err := level.GetDB()
	if err != nil {
		return false, err
	}
	return db.Has([]byte(key), nil)
}

// 写入数据
func (level *LevelDB) Write(key string, content []byte) error {
	db, err := level.GetDB()
	if err != nil {
		return err
	}
	return db.Put([]byte(key), content, nil)
}

// WriteObject 将对象以json形式写入key中
func (level *LevelDB) WriteObject(key string, body interface{}) error {
	db, err := level.GetDB()
	if err != nil {
		return err
	}
	if marshal, err := json.Marshal(body); err != nil {
		return err
	} else {
		return db.Put([]byte(key), marshal, nil)
	}
}

// 读取数据
func (level *LevelDB) Read(key string) ([]byte, error) {
	db, err := level.GetDB()
	if err != nil {
		return nil, err
	}
	if data, err := db.Get([]byte(key), nil); err != nil {
		return nil, err
	} else {
		return data, nil
	}
}

// ReadObject 读取数据到目标对象
func (level *LevelDB) ReadObject(key string, object interface{}) error {
	db, err := level.GetDB()
	if err != nil {
		return err
	}
	if data, err := db.Get([]byte(key), nil); err != nil {
		return err
	} else {
		return json.Unmarshal(data, object)
	}
}

// Delete 删除
func (level *LevelDB) Delete(key string) error {
	db, err := level.GetDB()
	if err != nil {
		return err
	}
	return db.Delete([]byte(key), nil)
}

// GetIterator 获取迭代器
func (level *LevelDB) GetIterator(start string, limit string) iterator.Iterator {
	if start != "" && limit != "" {
		return level.db.NewIterator(&util.Range{Start: []byte(start), Limit: []byte(limit)}, nil)
	}
	return level.db.NewIterator(nil, nil)
}

