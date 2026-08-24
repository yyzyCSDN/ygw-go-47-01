# Coordination 分布式协调服务

Coordination 是一个通用的分布式协调基础设施，提供 MVCC 线性化键值存储、
租约管理、watch 变更推送、分布式锁与选主能力。客户端通过会话接入，键值
变更按版本对外推送，锁与选主建立在租约之上，保证同一时刻只有一个持有者。

## 功能

- 键值存储：`PUT /api/kv/put`、`GET /api/kv/{key}`、快照 `GET /api/kv/snapshot`，
  每次写入分配单调版本号，读取支持按版本快照
- 租约管理：`POST /api/lease/create`、`/api/lease/renew`、
  `/api/lease/batch-renew`、`/api/lease/revoke`，到期自动回收
- 分布式锁：`POST /api/lock/acquire`、`/api/lock/release`，支持排队等待
  与租约绑定，租约过期自动释放
- watch 推送：`GET /api/watch?key=...&rev=...`，以 Server-Sent Events 推送
  变更，断线重连后从游标续推
- 选主：`POST /api/election/campaign`、`/api/election/handoff`、
  `/api/election/resign`，心跳续期与任期切换
- 会话：`POST /api/session/connect`、`/api/session/reconnect`，断线重连
  自动恢复租约与 watch
- 管理控制台：`GET /`（浏览器打开）

## 构建与运行

环境要求：Go 1.23+（vendor 目录已包含全部依赖，可离线构建）。

```bash
go build -mod=vendor -o coordination ./cmd/coordination
./coordination
```

服务默认监听 `127.0.0.1:18081`，可通过环境变量 `COORD_LISTEN` 调整。
启动后访问 `http://127.0.0.1:18081/` 打开控制台，`GET /healthz` 做健康检查。

## 前端控制台

页面文件位于 `web/console.html`，编译时通过 `go:embed` 打进二进制，运行后
访问根路径即可使用。控制台展示当前版本号、键/租约/锁/watch/会话数量、
选主状态与锁持有情况。

## Docker

```bash
docker build -f benzhi.Dockerfile -t coordination .
docker run --rm -p 8080:8080 coordination
```

镜像内关闭模块下载（GOPROXY=off、GOSUMDB=off），全部依赖走 vendor 离线构建。
