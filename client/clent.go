package client

import (
	"emperror.dev/errors"
	pb "github.com/ocfl-archive/dlza-manager-handler/handlerproto"
	"google.golang.org/grpc"
	"io"
)

func NewClerkHandlerClient(target string, opt grpc.DialOption) (pb.ClerkHandlerServiceClient, io.Closer, error) {
	connection, err := grpc.Dial(target, opt)
	if err != nil {
		return nil, nil, errors.WithStack(err)
	}
	return pb.NewClerkHandlerServiceClient(connection), connection, nil
}

func NewStorageHandlerHandlerClient(target string, opt grpc.DialOption) (pb.StorageHandlerHandlerServiceClient, io.Closer, error) {
	connection, err := grpc.Dial(target, opt)
	if err != nil {
		return nil, nil, errors.WithStack(err)
	}
	return pb.NewStorageHandlerHandlerServiceClient(connection), connection, nil
}

func NewUploaderHandlerClient(target string, opt grpc.DialOption) (pb.UploaderHandlerServiceClient, io.Closer, error) {
	connection, err := grpc.Dial(target, opt)
	if err != nil {
		return nil, nil, errors.WithStack(err)
	}
	return pb.NewUploaderHandlerServiceClient(connection), connection, nil
}

func NewCheckerHandlerClient(target string, opt grpc.DialOption) (pb.CheckerHandlerServiceClient, io.Closer, error) {
	connection, err := grpc.Dial(target, opt)
	if err != nil {
		return nil, nil, errors.WithStack(err)
	}
	return pb.NewCheckerHandlerServiceClient(connection), connection, nil
}

func NewDispatcherHandlerClient(target string, opt grpc.DialOption) (pb.DispatcherHandlerServiceClient, io.Closer, error) {
	connection, err := grpc.Dial(target, opt)
	if err != nil {
		return nil, nil, errors.WithStack(err)
	}
	return pb.NewDispatcherHandlerServiceClient(connection), connection, nil
}
