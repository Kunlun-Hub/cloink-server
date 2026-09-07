# Cloink 新版本 0.78.1 适配记录

核验日期：2026-09-07（UTC）。本记录对应本地工作区，不代表已经发布或部署。

## 范围和基线

- 工作范围为 `/root/cloink-new`；旧 `/root/cloink` 仓库及旧部署不作修改。
- 服务端及客户端内核的上游目标为 `0.78.1`，提交为
  `23a1487c26c5f00353c046bc818069189650dbfb`。
- 服务端集成分支为 `main`，通过合并提交保留上游历史和 Cloink 定制。
- 本地保留上游 `v0.78.1` 基准标签，供客户端构建沿提交历史解析版本；这不是发布新的 Cloink 安装包。
- 控制台版本独立于内核，仅适配新的调试任务参数，不用整套上游界面覆盖现有定制。
- Go 模块要求 Go 1.26.0，并在 `go.mod` 固定工具链为 Go 1.26.7。

## 定制功能适配

| 功能 | 处理 |
| --- | --- |
| 自托管流量日志 | 新的 SQLite 网络映射推送继续携带流量配置、分组开关、包计数、可配置上报间隔及账号/设备绑定的令牌。 |
| 中继选路和迁移 | 接入上游 `netevents`，保留中继权重、优先级、故障恢复及无缝迁移；仅更新流量配置时不清空中继配置。 |
| 自托管调试包 | 默认上传到本管理服务，适配显式 `upload_url` 和匿名化级别；保留上游远程任务默认拒绝的安全开关。 |
| 登录和账号设置 | 保留邮件/企业微信登录定制；更新账号复制及持久化测试以覆盖定制默认值和新增字段。 |
| 品牌和汉化 | 保留 Cloink 配置目录、桌面入口、更新签名及安装包下载逻辑，适配新权限弹窗和 Linux polkit 包装。 |
| Windows 资源 | CLI 资源生成显式传入 `goversioninfo -64`，防止 `resources_windows_amd64.syso` 实际包含 i386 资源而导致链接失败。 |

`0.78.1` 客户端默认不接受远程调试任务。需要该功能的设备由管理员显式执行：

```sh
cloink up --allow-remote-jobs
```

不通过恢复旧的默认允许行为来绕过此限制。

## 已完成验证

| 验证 | 结果 |
| --- | --- |
| `go test -tags devcert -timeout 30m ./...` | 通过，244 个有测试的包，无失败。 |
| SQLite 网络映射集成测试 | `NETBIRD_STORE_ENGINE=sqlite go test -tags 'devcert integration' -timeout 8m ./integration_tests/management/network_map_db/...` 通过。 |
| 竞态测试 | 网络映射控制器、管理 gRPC、中继客户端三个包的 `go test -race -tags devcert` 通过。 |
| 增量 Go lint | `make lint` 通过，0 issues。 |
| 控制台 | 使用仓库已有的 `package-lock.json` 执行 `npm ci`；汉化检查、TypeScript、修改文件 ESLint、生产构建通过。移除误生成的 pnpm 锁文件和工作区文件。 |
| 桌面前端 | 使用 Go 1.26.7 编译的 Wails CLI 重新生成绑定；生产构建、汉化检查、ESLint、TypeScript 通过。 |
| Linux amd64 | CLI、combined、management、signal、relay、桌面 UI 编译通过；CLI 实际输出版本 `0.78.1`，combined/relay 帮助命令正常退出。 |
| 跨平台编译 | Windows amd64 CLI/UI、macOS arm64 CLI、Linux arm64 CLI 编译通过。 |
| Android 内核 | 新服务端工作区中的 `GOOS=android GOARCH=arm64 CGO_ENABLED=0 go build ./client/android` 通过；这不是 APK 验证。 |

本机 `make test-unit` 默认的 10 分钟超时不足以跑完管理服务测试，因此本次保持
`devcert` 和全包覆盖，仅将本次命令的超时扩大到 30 分钟；未修改 Makefile 或跳过测试。

Wails 绑定是生成文件，不应手改 TypeScript 类型来掩盖 Go 服务接口变化。
旧的 Go 1.25 构建的生成器会对新代码发出 Go 版本告警；应在新工具链下重新编译生成器后生成。

本地验证日志位于 `/tmp/cloink-0.78.1-*.log`，二进制位于
`/tmp/cloink-0.78.1-bin/`。这些是未签名的本地验证产物，不是可对外发布的安装包。

## 集成要求与待验证事项

1. **Android 应用必须固定到定制内核提交。** 以 `cloink-android` 提交中记录的 `netbird`
   子模块指针为准，并验证该提交包含上述上游基线及 Cloink 定制。同步时从本地
   `cloink-server` 获取已提交的源码，保留子模块的 GitHub 远程地址。不能只修改显示版本，
   也不能指向丢失 Cloink 定制的纯上游提交。
2. **未验证 Android APK、macOS 桌面 UI 和 FreeBSD 原生构建。** 当前本机没有 Android SDK/NDK
   或相应原生平台环境；FreeBSD 的 `wgctrl` 依赖 CGO，不能用 Linux 上的
   `CGO_ENABLED=0` 交叉编译结果代替其原生构建验证。
3. **全量 lint 仍有 13 项既有/依赖目录告警。** 报告涉及原有中继注册、流量令牌常量、
   企业微信模板、邀请邮件测试、流量管理器、未使用辅助函数及 `proxy/web/node_modules`
   内的依赖代码。未修改检查阈值、排除规则或无关业务实现以隐藏告警。
4. **桌面全量格式检查有一项既有失败。**
   `client/ui/frontend/src/modules/auto-update/UpdateVersionCard.tsx` 未被此次合并修改，
   但未通过现有 Prettier 检查；其余桌面检查通过。
5. **本轮仅做本地提交和子模块同步。** 不推送、不部署，也不创建新的 Cloink 发布。
   GitHub CI、正式安装包、签名以及运行中部署的验收尚未执行。

## 发布前顺序

1. 确认三个新版本仓库的本地提交和工作区状态，核对子模块与定制内核提交一致。
2. 重新构建 AAR/APK 并验证原有网络回调、账号切换与升级提示。
3. 在各自原生构建环境补齐平台验证，单独处理已确认的既有检查问题。
4. 如需发布，先推送服务端提交及上游基准标签，再推送引用它的 Android 提交，避免子模块指向远端不存在的提交。
5. 观察 CI 终态，核对安装包版本、签名及下载文件名；如需部署，仅操作新版本镜像和部署。
