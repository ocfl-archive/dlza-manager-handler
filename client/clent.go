package client

import (
	"emperror.dev/errors"
	pb "github.com/ocfl-archive/dlza-manager-handler/handlerproto"
	"google.golang.org/grpc"
	"io"
)

const maxMsgSize = 1024 * 1024 * 12

func NewClerkHandlerClient(target string, opt grpc.DialOption) (pb.ClerkHandlerServiceClient, io.Closer, error) {
	connection, err := grpc.NewClient(target, opt, grpc.WithDefaultCallOptions(grpc.MaxCallRecvMsgSize(maxMsgSize), grpc.MaxCallSendMsgSize(maxMsgSize)))
	if err != nil {
		return nil, nil, errors.WithStack(err)
	}
	return pb.NewClerkHandlerServiceClient(connection), connection, nil
}

func NewStorageHandlerHandlerClient(target string, opt grpc.DialOption) (pb.StorageHandlerHandlerServiceClient, io.Closer, error) {
	connection, err := grpc.NewClient(target, opt, grpc.WithDefaultCallOptions(grpc.MaxCallRecvMsgSize(maxMsgSize), grpc.MaxCallSendMsgSize(maxMsgSize)))
	if err != nil {
		return nil, nil, errors.WithStack(err)
	}
	return pb.NewStorageHandlerHandlerServiceClient(connection), connection, nil
}

func NewUploaderHandlerClient(target string, opt grpc.DialOption) (pb.UploaderHandlerServiceClient, io.Closer, error) {
	connection, err := grpc.NewClient(target, opt, grpc.WithDefaultCallOptions(grpc.MaxCallRecvMsgSize(maxMsgSize), grpc.MaxCallSendMsgSize(maxMsgSize)))
	if err != nil {
		return nil, nil, errors.WithStack(err)
	}
	return pb.NewUploaderHandlerServiceClient(connection), connection, nil
}

func NewCheckerHandlerClient(target string, opt grpc.DialOption) (pb.CheckerHandlerServiceClient, io.Closer, error) {
	connection, err := grpc.NewClient(target, opt, grpc.WithDefaultCallOptions(grpc.MaxCallRecvMsgSize(maxMsgSize), grpc.MaxCallSendMsgSize(maxMsgSize)))
	if err != nil {
		return nil, nil, errors.WithStack(err)
	}
	return pb.NewCheckerHandlerServiceClient(connection), connection, nil
}

func NewDispatcherHandlerClient(target string, opt grpc.DialOption) (pb.DispatcherHandlerServiceClient, io.Closer, error) {
	connection, err := grpc.NewClient(target, opt, grpc.WithDefaultCallOptions(grpc.MaxCallRecvMsgSize(maxMsgSize), grpc.MaxCallSendMsgSize(maxMsgSize)))
	if err != nil {
		return nil, nil, errors.WithStack(err)
	}
	return pb.NewDispatcherHandlerServiceClient(connection), connection, nil
}
