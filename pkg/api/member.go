package api

import (
	"context"
	"time"

	pb "gitlab.com/sdce/protogo"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
)

const (
	apiCallLiveTime = 5 * time.Second
)

func (s *Server) LockAccountBalance(ctx context.Context, lr *LockBalance) (err error) {

	apiCtx, cancel := context.WithTimeout(ctx, apiCallLiveTime)
	defer cancel()
	req, err := s.newLockBalanceRequest(ctx, lr)
	if err != nil {
		//log.Errorf("failed to create lockbalance req: %v", err)
		return err
	}
	res, err := s.Member.DoLockBalance(apiCtx, req)
	if err != nil {
		err = grpc.Errorf(codes.Unknown, err.Error())
		return
	}
	result := res.GetResult()
	var code codes.Code
	switch result {
	case pb.LockBalanceResult_INVALID_LOCK_MONEY_RESULT:
		code = codes.Internal
	case pb.LockBalanceResult_LOCK_SUCCESSFUL:
		code = codes.OK
	case pb.LockBalanceResult_LOCK_INSUFFICIENT_BALANCE:
		code = codes.FailedPrecondition
	case pb.LockBalanceResult_LOCK_INVALID_SOURCE:
		code = codes.InvalidArgument
	case pb.LockBalanceResult_LOCK_ACCOUNT_NOT_FOUND:
		code = codes.InvalidArgument
	case pb.LockBalanceResult_LOCK_UNSUCCESSFUL_ROLL_BACK:
		code = codes.FailedPrecondition
	}
	if code != codes.OK {
		err = grpc.Errorf(code, "apiLockBalance() subroutine error: "+result.String())
		return
	}
	return
}

func (s *Server) ReleaselockedBalance(ctx context.Context, req *pb.ReleaseLockedBalanceRequest) (err error) {
	apiCtx, cancel := context.WithTimeout(ctx, apiCallLiveTime)
	defer cancel()
	ret, err := s.Member.DoReleaseLockedBalance(apiCtx, req)
	if err != nil {
		err = grpc.Errorf(codes.Unknown, err.Error())
		return
	}
	if !ret.Success {
		err = grpc.Errorf(codes.Unknown, err.Error())
	}
	return
}

func (s *Server) FindMemberAccount(ctx context.Context, member *pb.UUID, coin *pb.UUID) ([]*pb.AccountDefined, error) {
	apiCtx, cancel := context.WithTimeout(ctx, apiCallLiveTime)
	defer cancel()
	// build lock req
	req := &pb.SearchAccountsRequest{
		MemberId:   member,
		Currencies: []*pb.UUID{coin},
		Status:     []pb.AccountDefined_AccountStatus{pb.AccountDefined_Primary},
		Paging: &pb.PaginationRequest{
			PageIndex: 0,
			PageSize:  10,
		},
	}

	res, err := s.Member.DoSearchAccounts(apiCtx, req)
	if err != nil {
		err = grpc.Errorf(codes.Unknown, err.Error())
		return []*pb.AccountDefined{}, err
	}
	return res.Account, nil
}

func (s *Server) FindMember(ctx context.Context, memberId *pb.UUID) (*pb.MemberDefined, error) {
	apiCtx, cancel := context.WithTimeout(ctx, apiCallLiveTime)
	defer cancel()
	req := &pb.FindMemberRequest{
		MemberId: memberId,
	}
	res, err := s.Member.DoFindMember(apiCtx, req)
	if err != nil {
		return nil, err
	}
	return res.MemberDefined, nil
}

func (s *Server) AddPending(ctx context.Context, in *pb.AddPendingRequest) (*pb.AddPendingResponse, error) {
	return s.Member.DoAddPending(ctx, in)
}

func (s *Server) ReleasePending(ctx context.Context, in *pb.ReleasePendingRequest) (*pb.ReleasePendingResponse, error) {
	return s.Member.DoReleasePending(ctx, in)
}
