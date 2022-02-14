package goleveldb

import (
	"log"
	"testing"
)

func TestLevelDB_Write(t *testing.T) {
	db := NewLevelDB("db")
	err := db.Write("aa", []byte("456"))
	if err == nil {
		read, _ := db.Read("aa")
		log.Println(string(read))
	}
}

type Demo struct {
	Id   int
	Name string
}

func TestLevelDB_WriteObject(t *testing.T) {
	db := NewLevelDB("db")
	key := "object"
	demo := Demo{
		Id:   1,
		Name: "tom",
	}
	err := db.WriteObject(key, &demo)
	if err == nil {
		var demo2 = Demo{}
		_ = db.ReadObject(key, &demo2)
		log.Println(demo2)
	}
}

func TestLevelDB_Delete(t *testing.T) {
	db := NewLevelDB("db")
	key := "object"
	db.Delete(key)
	has, _ := db.Has(key)
	log.Println(has)
}


