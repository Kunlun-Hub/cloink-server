import type { Translations } from "./en";

const zhCN: Translations = {
  // Page titles
  "auth.pageTitle": "需要认证 - NetBird 服务",
  "error.pageTitle": "{title} - NetBird 服务",

  // App (Authentication page)
  "auth.title": "需要认证",
  "auth.description": "你正在尝试访问的服务受到保护，请先完成认证。",
  "auth.signInWithSso": "使用 SSO 登录",
  "auth.password": "密码",
  "auth.pin": "PIN",
  "auth.enterPassword": "输入密码",
  "auth.enterPinCode": "输入 PIN 码",
  "auth.signIn": "登录",
  "auth.submit": "提交",
  "auth.verifying": "验证中...",
  "auth.authenticated": "已认证",
  "auth.loadingService": "正在加载服务...",
  "auth.failed": "认证失败，请重试。",
  "auth.error": "发生错误，请重试。",

  // Error page
  "error.code": "错误 {code}",
  "error.you": "你",
  "error.proxy": "代理",
  "error.destination": "目标",
  "error.connected": "已连接",
  "error.unreachable": "不可达",
  "error.refreshPage": "刷新页面",
  "error.documentation": "文档",
  "error.requestId": "请求 ID",
  "error.timestamp": "时间戳",

  // Common
  "common.poweredBy": "由",

  // Proxy error messages (sent as i18n keys from Go backend)
  "proxyError.serviceNotFound": "服务未找到",
  "proxyError.serviceNotFoundMessage": "无法找到请求的服务。请检查 URL、尝试刷新，或检查对等节点是否正在运行。如仍有问题，请参阅我们的文档。",
  "proxyError.loopDetected": "检测到环路",
  "proxyError.loopDetectedMessage": "此节点是请求服务的目标。请直接访问后端，而不是从同一台机器访问公共服务 URL。",
  "proxyError.requestTimeout": "请求超时",
  "proxyError.requestTimeoutMessage": "尝试访问服务时请求超时。请刷新页面重试。",
  "proxyError.requestCanceled": "请求已取消",
  "proxyError.requestCanceledMessage": "请求在完成之前被取消。请刷新页面重试。",
  "proxyError.configurationError": "配置错误",
  "proxyError.configurationErrorMessage": "由于配置问题，无法处理请求。请刷新页面重试。",
  "proxyError.proxyNotConnected": "代理未连接",
  "proxyError.proxyNotConnectedMessage": "代理未连接到 NetBird 网络。请稍后重试或联系管理员。",
  "proxyError.serviceOverloaded": "服务过载",
  "proxyError.serviceOverloadedMessage": "服务当前正在处理过多请求。请稍后重试。",
  "proxyError.serviceUnavailable": "服务不可用",
  "proxyError.serviceUnavailableMessage": "连接服务被拒绝。请验证服务是否正在运行并重试。",
  "proxyError.peerNotConnected": "节点未连接",
  "proxyError.peerNotConnectedMessage": "无法建立与节点的连接。请确保节点正在运行并已连接到 NetBird 网络。",
  "proxyError.connectionError": "连接错误",
  "proxyError.connectionErrorMessage": "连接服务时发生意外错误。请稍后重试。",

  // Access denied messages
  "proxyError.accessDenied": "访问被拒绝",
  "proxyError.accessDeniedMessage": "您无权访问此服务",
  "proxyError.authError": "认证过程中发生错误",
  "proxyError.accountPendingApproval": "您的账户正在等待管理员批准",
  "proxyError.accountBlocked": "您的账户已被封禁",
  "proxyError.serviceConfigError": "服务配置错误",
  "proxyError.invalidSessionToken": "无效的会话令牌",
  "proxyError.authServiceUnavailable": "认证服务不可用",
};

export default zhCN;
