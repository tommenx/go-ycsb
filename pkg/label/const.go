package label

var (
	StoreAddr      string
	JobName        string
	Log            string
	QPS            int
	OperationCount int
)

const (
	PREFIX_LOG_QPS = iota
	PREFIX_REQUEST_QPS
	PREFIX_LOG_OPERATION
	PREFIX_REQUEST_OPERATION
)
