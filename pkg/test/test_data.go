package test

import (
	"gitlab.com/sdce/exlib/exutil"
	pb "gitlab.com/sdce/protogo"
)

var (
	fid, _ = exutil.AtoUUID("5c7f72aaae9411b00e000491")
	bid, _ = exutil.AtoUUID("5c7f6bc09e7405297329f087")
	qid, _ = exutil.AtoUUID("5c8467223c1b73fc9ba602d6")

	AUDRef = &pb.CurrencyRef{
		Symbol:  "AUD",
		Decimal: 2,
		Id:      qid,
		Type:    pb.Currency_CURRENCY_FIAT,
	}

	BTCRef = &pb.CurrencyRef{
		Symbol:  "BTC",
		Decimal: 8,
		Id:      bid,
		Type:    pb.Currency_CURRENCY_BTC,
	}

	FakeInstrumentRef = &pb.InstrumentRef{
		Id:    fid,
		Code:  "Tuzi-RMB",
		Name:  "Tuzi/Rmb",
		Base:  BTCRef,
		Quote: AUDRef,
	}

	user1, _ = exutil.AtoUUID("5c7bc423a22a98e52b5ac1e4")
	user2, _ = exutil.AtoUUID("5c846a0f3c1b73fc9ba602f0")
	user3, _ = exutil.AtoUUID("5c8469933c1b73fc9ba602ec")
	user4, _ = exutil.AtoUUID("5c7e0420ae3e23c93982b684")
)
