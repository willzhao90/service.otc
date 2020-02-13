package repository

import (
	"go.mongodb.org/mongo-driver/bson"
)

// TimeRangeFilter creates a filter for int64 time in (start, end]
func TimeRangeFilter(start, end int64) bson.M {
	return bson.M{
		"$gte": start,
		"$lte": end}
}
