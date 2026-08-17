package email

import "github.com/netbirdio/netbird/management/server/types"

func DefaultTemplates() map[string]types.EmailTemplate {
	return map[string]types.EmailTemplate{
		string(types.EmailTemplateInviteUser): {
			Enabled: true,
			Subject: "你被邀请加入 {{.account.name}}",
			BodyHTML: `<p>{{.invite.created_by_name}} 邀请你加入 {{.account.name}}。</p>
<p><a href="{{.invite.url}}">接受邀请</a></p>
<p>邀请将在 {{.invite.expires_at}} 过期。</p>`,
			BodyText: "{{.invite.created_by_name}} 邀请你加入 {{.account.name}}。\n\n接受邀请：{{.invite.url}}\n\n邀请将在 {{.invite.expires_at}} 过期。",
		},
		string(types.EmailTemplateCreateUser): {
			Enabled: true,
			Subject: "你的 Cloink 账号已创建",
			BodyHTML: `<p>你的 Cloink 账号已创建。</p>
<p>账号：{{.user.email}}</p>
<p><a href="{{.dashboard.url}}">打开控制台</a></p>`,
			BodyText: "你的 Cloink 账号已创建。\n\n账号：{{.user.email}}\n控制台：{{.dashboard.url}}",
		},
		string(types.EmailTemplateInviteAccepted): {
			Enabled: true,
			Subject: "{{.user.email}} 已接受邀请",
			BodyHTML: `<p>{{.user.name}}（{{.user.email}}）已接受邀请并完成注册。</p>
<p>时间：{{.time}}</p>`,
			BodyText: "{{.user.name}}（{{.user.email}}）已接受邀请并完成注册。\n\n时间：{{.time}}",
		},
		string(types.EmailTemplateUserPendingApproval): {
			Enabled: true,
			Subject: "有新用户等待审批",
			BodyHTML: `<p>有新用户等待审批。</p>
<p>用户：{{.user.name}}（{{.user.email}}）</p>
<p><a href="{{.approval.url}}">前往审批</a></p>`,
			BodyText: "有新用户等待审批。\n\n用户：{{.user.name}}（{{.user.email}}）\n审批入口：{{.approval.url}}",
		},
		string(types.EmailTemplateDevicePendingApproval): {
			Enabled: true,
			Subject: "有新设备等待审批",
			BodyHTML: `<p>有新设备等待审批。</p>
<p>设备：{{.device.name}}</p>
<p>用户：{{.device.user_email}}</p>
<p><a href="{{.approval.url}}">前往审批</a></p>`,
			BodyText: "有新设备等待审批。\n\n设备：{{.device.name}}\n用户：{{.device.user_email}}\n审批入口：{{.approval.url}}",
		},
	}
}

func MergeTemplates(custom map[string]types.EmailTemplate) map[string]types.EmailTemplate {
	templates := DefaultTemplates()
	for key, value := range custom {
		templates[key] = value
	}
	return templates
}
