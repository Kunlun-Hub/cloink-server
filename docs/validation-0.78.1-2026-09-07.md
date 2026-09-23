# Cloink 0.78.1 完整本地验证报告

验证日期：2026-09-07，UTC。

## 结论

**不能判定为“全部适配完成”或“可以正式发布”。**

当前提交的主要构建、自动化测试、管理端登录、真实设备联网、权限执行、调试上传和浏览器文件下载通过验证。但是，实测发现 Linux 用户态 WireGuard 接口搭配原生防火墙时存在流量采集缺口；另有全量静态检查未通过，以及本机无法完成的原生平台和真实外部服务验证。

本次没有修改业务源码、提交新的 Git commit、推送仓库、发布镜像或部署生产服务。仓库内新增文件仅为本报告。旧 `/root/cloink` 和现有部署保持不变。

## 1. 验证基线

| 仓库 | 分支 | 提交 |
| --- | --- | --- |
| cloink-server | main | `3dec1875fa86c86f30b1f20ea9b012a8594952b4` |
| cloink-dashboard | main | `1662abc00c3e7e7105659b16be7d3d6c1404fb71` |
| cloink-android | main | `04a0fe811ec8d0de33e0b19e55e52dfabc25e3f3` |

Android 的 `netbird` gitlink 与服务端提交完全一致。上游目标为本地 `v0.78.1`，不是新创建的 Cloink 正式发行版。

主要证据目录：`/tmp/cloink-0781-validation-20260907`。下文日志路径均相对于该目录。

隔离测试使用独立 Docker 网络和 `cloink.validation=0781-20260907` 标签。发布到宿主机的测试端口只绑定 `127.0.0.1`。联网和特权测试全部在容器内执行，未在宿主机运行客户端 `up`。

测试服务镜像以缓存运行时叠加当前提交新构建的二进制；容器内服务端、客户端、中继二进制与本地构建文件的 SHA256 一致，见 `runtime-binary-hashes.txt`。这不等同于完整复现正式 CI 镜像构建、签名或发布流程。

## 2. 自动化与构建结果

| 项目 | 结果 | 证据 |
| --- | --- | --- |
| Go 全量非缓存单元测试 | 244 个有测试的包通过 | `go-unit.log` |
| Go 特权测试 | 114 个有测试的包通过；部分复用同轮成功结果缓存 | `go-privileged-validated.log` |
| SQLite 网络映射集成测试 | 19 个测试通过，0 跳过 | `go-sqlite-integration.jsonl` |
| Race 检测 | 网络映射控制器、管理 gRPC、中继客户端三个包通过 | `go-race.log` |
| Go 变更范围 lint | 0 issues | `go-lint-changed.log` |
| Go 全量 lint | 未通过：13 项 | `go-lint-all.log` |
| Dashboard i18n、类型检查、生产构建 | 通过 | `dashboard-i18n.log`、`dashboard-types.log`、`dashboard-build.log` |
| Dashboard 全量 lint | 未通过：196 errors、452 warnings | `dashboard-lint.log` |
| 桌面前端 i18n、ESLint、类型检查、构建 | 通过 | `desktop-check.log`、`desktop-build.log` |
| 桌面前端格式检查 | 未通过：1 个文件 | `desktop-check.log` |
| Linux amd64 CLI、服务端、中继 | 当前提交重新构建并实际运行 | `runtime/`、`runtime-build.exit` |
| Linux amd64 桌面 UI | 构建通过；非特权用户下启动 Wails/WebKit 冒烟通过 | `build-ui-linux.log`、`linux-ui-smoke.log` |
| Windows amd64 CLI、UI | 两个 PE32+ 二进制构建通过 | `build-cli-windows.log`、`build-ui-windows.log` |
| Windows NSIS 安装包 | 生成成功；未做 Windows 原生安装/卸载 | `windows-nsis.log` |
| macOS arm64 CLI、Linux arm64 CLI | 交叉编译通过 | `build-cli-darwin.log`、`build-cli-linux-arm64.log` |
| FreeBSD amd64 CLI | 本机 CGO=0 交叉编译未通过，不能据此判定原生 FreeBSD 支持 | `build-cli-freebsd.log` |

主要 Go 命令：

```sh
go test -count=1 -tags devcert -timeout 30m ./...
NETBIRD_STORE_ENGINE=sqlite go test -count=1 -json -tags 'devcert integration' -timeout 8m ./integration_tests/management/network_map_db/...
go test -count=1 -race -tags devcert -timeout 10m ./management/internals/controllers/network_map/controller ./management/internals/shared/grpc ./shared/relay/client
```

特权测试采用项目规定的过滤包集和 `-tags 'devcert privileged'`，只在隔离容器中运行。最终使用当前提交的可写临时源码快照及本轮构建的嵌入式前端，避免测试用临时文件写入正式工作区。

Linux 桌面冒烟使用独立 HOME、非特权用户和不存在的测试 daemon socket，确认 GUI 能初始化，没有连接任何实际宿主机 daemon。20 秒后由 `timeout` 主动结束，退出码 124 是测试的结束方式，不能等同于桌面连接、提权和安装功能全部通过。

## 3. Android APK 实物验证

使用官方 SDK/NDK 固定版本下载包，并核对仓库元数据中的 SHA1。构建时使用隔离 SDK 目录，没有读取正式签名密钥。

- NDK：`23.1.7779620`；SDK / target：35；min SDK：26。
- 原始 `./build-android-lib.sh 0.78.1` 最终执行成功，生成四 ABI AAR。
- `:app:assembleDebug` 成功，`:app:testDebugUnitTest` 的 2 个测试全部通过。
- APK 包名：`io.cloink.client`；versionName：`0.78.1`；versionCode：`9999`。
- ABI：`arm64-v8a`、`armeabi-v7a`、`x86`、`x86_64`。
- `apksigner verify` 通过，属于 Debug 签名，不是正式发布签名。
- `zipalign -c -P 16 -v 4` 通过；提取出的所有原生库 LOAD 段均为 `0x4000` 对齐。
- `:app:lintDebug` 未通过：5 errors、147 warnings。

产物：`/root/cloink-new/cloink-android/app/build/outputs/apk/debug/app-debug.apk`。

证据：`android-aar-retry.log`、`android-gradle-verified-deps.log`、`android-apk-badging.log`、`android-apk-signature.log`、`android-apk-alignment.log`、`android-elf-alignment.log`。

首次 gomobile 执行曾出现生成模块缺少 module 声明的错误；未改代码的诊断执行和原始脚本重跑均成功，不能把首次异常隐藏为“始终一次成功”。Gradle 下载过程也出现 Maven TLS 中断；固定版本依赖重试并核对 SHA1 后，编译和测试成功。最终 Gradle 总任务的非零退出来自上述 lint，而不是 APK 编译失败。

## 4. 实际运行链路

### 管理端、身份与权限

- 初次初始化成功；初始化后再次调用 setup 被拒绝。
- 无凭证访问管理 API 返回 401。
- Playwright 使用实际页面完成嵌入式 IdP 邮箱/密码登录，看到真实注册的客户端。
- 非管理员角色对中继 setup-token、版本发布创建、账户设置修改返回 403。
- 服务用户的部分只读访问是权限实现明确允许的行为，没有把它误判为写权限绕过。

证据：`lab-prepare.log`、`browser-validated.log`、`lab-permissions-persistence-validated.log`。

### 实际设备网络与中继

- 两个 Linux 客户端注册、管理与信令连接成功；实际 P2P ping 5/5 成功。
- 另两个客户端强制中继，实际选择优先级 90 的中继，而不是优先级 50/30。
- 停止优先级 90 的真实中继容器后，连接回落至优先级 50，实际数据通信恢复。
- 本轮单次故障观察中，恢复约需 65 秒，不应表述成“无感切换”。
- 高优先级中继恢复后，日志确认 home relay 已切回。现存连接可能保留在旧 foreign relay 上，属于当前实现的保活策略；重新建立的 peer session 已验证使用恢复后的高优先级中继。
- 真实中继 TCP 传输 804,616 字节，接收 SHA256 与发送文件一致；IPv6 ping 也有实际成功记录。
- 撤销默认访问策略后，真实 overlay 流量被阻断；恢复策略后，流量恢复。

证据：`lab-p2p.log`、`lab-relay-continuous-ping.log`、`lab-relay-failover.json`、`lab-forced-userspace-firewall.log`、`lab-permissions-persistence-validated.log`。

最初中继脚本错误地要求所有存量连接必须立即迁回，并在清理已退出的 ping 时得到非零返回，因此 `lab-network.log` 不是“全脚本通过”的证据。恢复验证以实际 home relay 日志、新会话和实际数据传输为准。

### 远程调试

- 未启用远程任务时，任务由实际客户端拒绝，而不是仅在 UI 中隐藏。
- 非法 `anonymize_level` 被 API 拒绝。
- 显式启用 `--allow-remote-jobs` 后，严格匿名化调试包在自托管服务上传成功。
- 正向上传使用 HTTPS，客户端信任仅安装于测试容器的临时 CA，没有关闭证书校验。

证据：`lab-live.log`、`lab-job-refused.json`、`lab-job-uploaded.json`。

### 发布文件与浏览器下载

- 上传当前提交 CLI 打包得到的真实 `Cloink-0.78.1-linux-amd64.tar.gz`。
- 服务端计算 SHA256 与源文件一致；GET、HEAD、Range 206、Content-Length 和 Content-Disposition 验证通过。
- 未签名的 latest 发布被拒绝，未混入公共 latest 列表。
- Playwright 点击真实版本列表下载按钮，浏览器建议文件名及完整 `.tar.gz` 扩展名正确，下载字节 SHA256 一致。
- 管理进程重启后 PAT、账户设置、发布元数据和文件读取通过验证。
- 按标准挂载 `/var/lib/netbird` 后，删除并重建服务容器，再次确认 PAT、发布元数据、实际文件字节和文件名响应头仍然正确。

证据：`lab-live.log`、`browser-validated.log`、`lab-permissions-persistence-validated.log`、`lab-recreate-final.log`。

测试环境最初只挂载了自定义数据库目录 `/nb/data`，未挂载独立的默认 artifact 根目录，重建容器后曾得到 404。核查后补齐标准 artifact 挂载并重新上传、重建、核验；这项纠正属于测试环境配置，不是修改业务源码后掩盖失败。正式配置同样必须持久化 `/var/lib/netbird/version-releases`，或者正确设置并挂载 `NB_VERSION_RELEASES_DIR`。

浏览器 `/relays`、`/events/traffic`、`/settings?tab=version-releases`、`/install` 实际打开通过；仍能观察到许可探测端点的 CORS/404 和部分 412 控制台提示，没有把它们说成“控制台完全无错误”。本轮未用正式发布私钥签署新的 0.78.1 元数据。

## 5. 必须处理的运行问题：Linux 混合模式流量采集

**现象可复现，且有当前二进制的配置对照，不是仅凭静态检查推测。**

### 失败组合

- Linux，`NB_WG_KERNEL_DISABLED=true`，即用户态 WireGuard bind。
- 没有强制用户态防火墙，实际选择原生 iptables。
- 客户端 trace 明确收到 `enabled=true`、`interval=3s` 的流量配置。
- 实际中继 TCP 文件传输成功，SHA256 一致，但等待 80 秒仍未得到该 reporter 的 TCP 流量记录。

证据：`lab-remaining-network.log`、`client-three-native-firewall.log`、`client-four-native-firewall.log`。

### 对照组合

仅在测试容器增加 `NB_FORCE_USERSPACE_FIREWALL=true`，保留同一提交和客户端状态后：

- 同样的 TCP 传输产生了自托管流量记录。
- 实测窗口约 3 秒，包/字节计数非零。
- 将上报组收窄为 source 客户端：该客户端产生新记录，被排除的 target 客户端不产生新记录。
- 访问策略撤销和恢复仍在实际流量路径生效。

证据：`lab-forced-userspace-firewall.log`、`lab-flow-group-filter.json`、`lab-flow-counters.json`。

### 代码定位

- `client/firewall/create_linux.go:65`：原生防火墙配合 userspace bind 时只安装 `uspfilter.HooksFilter`，没有将 flow logger 接入这个路径。
- `client/internal/netflow/manager.go:71`：Linux conntrack 采集器又只在非 userspace bind 时创建。
- 两个条件叠加，当前混合模式没有负责产出流量事件的采集路径。
- 与升级前第一父提交比较，防火墙创建路径发生了重构，而上述 conntrack 条件没有对应改变。未另行启动旧提交二进制做历史端到端对照，历史归因应与当前配置 A/B 证据区分。

强制用户态防火墙是本次定位用的对照条件，**不是已经修复默认模式**，也没有作为生产配置变更实施。应先补齐这个采集路径及对应回归测试，再重新运行原始失败组合。

## 6. 其他未通过项及边界

- Go 全量 lint：13 项；变更范围 lint 为 0 issues。
- Dashboard：196 个错误全部位于本次 dashboard 提交未改变的文件中，不能把它们归因于这 4 个改动文件，但全量门禁确实没有通过。
- 桌面格式：`client/ui/frontend/src/modules/auto-update/UpdateVersionCard.tsx`，历史未变文件。
- Android lint：两个 theme 的 API 27 属性与 min SDK 26 不匹配、TileService 过期调用、TroubleshootFragment 两处未 remember 的状态；涉及文件在本次 Android 提交中没有改动。没有顺带修复这些历史项。
- FreeBSD 的 `CGO_ENABLED=0` 交叉编译报 `wgfreebsd.New` 未定义，需要合适的原生/交叉 CGO 环境继续验证，不能宣称 FreeBSD 已验证完成。
- 没有执行 Windows/macOS/FreeBSD 原生安装、升级、卸载及系统服务行为，也没有 Android 真机安装/VPN 运行和 16 KB 页设备实测。
- 没有执行真实企业 IdP、SMTP、跨运营商公网 NAT、外部数据库和依赖第三方服务的全部端到端流程。
- 没有运行新的 GitHub 发布流程，也未验证生产签名、正式安装器分发或生产镜像部署。

Windows 本地安装器位于 `windows-package/cloink-installer.exe`；各平台 CLI/UI 位于 `matrix/`。这些是本地验证产物，不是可直接分发的正式发布件。

## 7. 收尾

2026-09-07 08:15 UTC，12 个测试容器和独立测试网络已按本轮专用标签回收，剩余测试容器为 0，结果记录在 `cleanup.log`。原有容器 ID 对照缺失数为 0。会话开始时原有的 `netbird-server`、`netbird-proxy` 已处于重启状态；本次没有调整或重启它们。

原有容器身份对照记录在 `container-baseline.txt` 和 `container-after-cleanup.txt`。日志、脚本、截图、临时 SDK 和验证产物保留在上述审计目录，不进入 Git。

下一步优先处理第 5 节的采集缺口，然后重新验证原生防火墙和用户态防火墙两种组合；完成静态门禁及目标平台原生测试前，不应将这次结果描述为“全部适配完成”。
