package helper

func pointer[V any](v V) *V {
	return &v
}
