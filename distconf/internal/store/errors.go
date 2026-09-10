package store

import "errors"

// CAS 版本冲突：expected_version 与当前版本不一致。
var ErrCASConflict = errors.New("cas conflict: expected version does not match current version")

// 服务实例不存在（通常是租约已过期，需要重新 Register）。
var ErrInstanceNotFound = errors.New("service instance not found (lease expired?)")

// 客户端请求的 from_revision 已经被压缩（超出历史保留窗口），
// 必须重新做一次全量 Get 后再从当前 revision 开始 Watch。
var ErrRevisionCompacted = errors.New("requested revision has been compacted; resync required")
