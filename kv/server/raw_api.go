package server

import (
	"context"

	"github.com/pingcap-incubator/tinykv/kv/storage"
	"github.com/pingcap-incubator/tinykv/proto/pkg/kvrpcpb"
)

// The functions below are Server's Raw API. (implements TinyKvServer).
// Some helper methods can be found in sever.go in the current directory

// RawGet return the corresponding Get response based on RawGetRequest's CF and Key fields
func (server *Server) RawGet(_ context.Context, req *kvrpcpb.RawGetRequest) (*kvrpcpb.RawGetResponse, error) {
	reader, err := server.storage.Reader(req.Context)
	if err != nil {
		return nil, err
	}
	val, _ := reader.GetCF(req.GetCf(), req.GetKey())
	if val == nil {
		return &kvrpcpb.RawGetResponse{
			RegionError: nil,
			Error:       "",
			Value:       nil,
			NotFound:    true,
		}, nil
	}
	return &kvrpcpb.RawGetResponse{
		RegionError: nil,
		Error:       "",
		Value:       val,
		NotFound:    false,
	}, nil
}

// RawPut puts the target data into storage and returns the corresponding response
func (server *Server) RawPut(_ context.Context, req *kvrpcpb.RawPutRequest) (*kvrpcpb.RawPutResponse, error) {
	err := server.storage.Write(req.Context, []storage.Modify{
		{
			Data: storage.Put{
				Key:   req.GetKey(),
				Value: req.GetValue(),
				Cf:    req.GetCf(),
			},
		},
	})
	if err != nil {
		return nil, err
	}
	return &kvrpcpb.RawPutResponse{
		RegionError: nil,
		Error:       "",
	}, nil
}

// RawDelete delete the target data from storage and returns the corresponding response
func (server *Server) RawDelete(_ context.Context, req *kvrpcpb.RawDeleteRequest) (*kvrpcpb.RawDeleteResponse, error) {
	err := server.storage.Write(req.Context, []storage.Modify{
		{
			Data: storage.Delete{
				Key: req.GetKey(),
				Cf:  req.GetCf(),
			},
		},
	})
	if err != nil {
		return nil, err
	}
	return &kvrpcpb.RawDeleteResponse{
		RegionError: nil,
		Error:       "",
	}, nil
}

// RawScan scan the data starting from the start key up to limit. and return the corresponding result
func (server *Server) RawScan(_ context.Context, req *kvrpcpb.RawScanRequest) (*kvrpcpb.RawScanResponse, error) {
	reader, err := server.storage.Reader(req.Context)
	if err != nil {
		return nil, err
	}
	iter := reader.IterCF(req.GetCf())
	iter.Seek(req.GetStartKey())
	pairs := []*kvrpcpb.KvPair{}
	for i, limit := 0, int(req.GetLimit()); i < limit && iter.Valid(); i++ {
		item := iter.Item()
		key := item.Key()
		val, err := item.Value()
		if err == nil {
			pairs = append(pairs, &kvrpcpb.KvPair{
				Error: nil,
				Key:   key,
				Value: val,
			})
		}
		iter.Next()
	}
	return &kvrpcpb.RawScanResponse{
		RegionError: nil,
		Error:       "",
		Kvs:         pairs,
	}, nil
}
