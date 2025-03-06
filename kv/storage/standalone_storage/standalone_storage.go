package standalone_storage

import (
	"github.com/Connor1996/badger"
	"github.com/pingcap-incubator/tinykv/kv/config"
	"github.com/pingcap-incubator/tinykv/kv/storage"
	"github.com/pingcap-incubator/tinykv/kv/util/engine_util"
	"github.com/pingcap-incubator/tinykv/proto/pkg/kvrpcpb"
)

// StandAloneStorage is an implementation of `Storage` for a single-node TinyKV instance. It does not
// communicate with other nodes and all data is stored locally.
type StandAloneStorage struct {
	db *badger.DB
}

func NewStandAloneStorage(conf *config.Config) *StandAloneStorage {
	return &StandAloneStorage{}
}

func (s *StandAloneStorage) Start() error {
	var err error
	opts := badger.DefaultOptions
	opts.Dir = "/tmp/badger"
	opts.ValueDir = "/tmp/badger"
	s.db, err = badger.Open(opts)
	if err != nil {
		panic(err.Error())
	}
	return nil
}

func (s *StandAloneStorage) Stop() error {
	if err := s.db.Close(); err != nil {
		panic(err.Error())
	}
	return nil
}

func (s *StandAloneStorage) Reader(ctx *kvrpcpb.Context) (storage.StorageReader, error) {
	return &StandAloneStorageReader{
		txn: s.db.NewTransaction(true),
	}, nil
}

func (s *StandAloneStorage) Write(ctx *kvrpcpb.Context, batch []storage.Modify) error {
	wb := engine_util.WriteBatch{}
	wb.Reset()
	for _, item := range batch {
		if val := item.Value(); val != nil {
			wb.SetCF(item.Cf(), item.Key(), val)
		} else {
			wb.DeleteCF(item.Cf(), item.Key())
		}
	}
	err := wb.WriteToDB(s.db)
	return err
}

type StandAloneStorageReader struct {
	txn *badger.Txn
}

func NewStandAloneStorageReader(txn *badger.Txn) *StandAloneStorageReader {
	return &StandAloneStorageReader{txn}
}

func (reader *StandAloneStorageReader) GetCF(cf string, key []byte) ([]byte, error) {
	val, err := engine_util.GetCFFromTxn(reader.txn, cf, key)
	if err != nil {
		return nil, nil
	}
	return val, nil
}

func (reader *StandAloneStorageReader) IterCF(cf string) engine_util.DBIterator {
	return engine_util.NewCFIterator(cf, reader.txn)
}

func (reader *StandAloneStorageReader) Close() {
	reader.txn.Discard()
}
