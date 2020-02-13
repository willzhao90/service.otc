package api

import (
	"context"

	pb "gitlab.com/sdce/protogo"
)

func (s *Server) FindInstrument(ctx context.Context, code string) (*pb.Instrument, error) {
	apiCtx, cancel := context.WithTimeout(ctx, apiCallLiveTime)
	defer cancel()
	req := &pb.GetInstrumentRequest{
		Identifier: &pb.GetInstrumentRequest_Name{
			Name: code,
		},
	}
	res, err := s.Trading.DoGetInstrument(apiCtx, req)
	if err != nil {
		return nil, err
	}
	return res, nil
}
