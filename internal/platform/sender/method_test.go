package sender

import "testing"

func TestClassifyMethod(t *testing.T) {
	tests := []struct {
		class   methodClass
		label   string
		methods []string
	}{
		{methodJoinRequest, "join_request", []string{
			"sendChatJoinRequestWebApp", "answerChatJoinRequestQuery", "approveChatJoinRequest", "declineChatJoinRequest",
		}},
		{methodMessage, "message", []string{
			"sendMessage", "editMessageText", "editMessageReplyMarkup", "editMessageCaption", "editMessageMedia",
			"sendPhoto", "sendDocument", "sendVideo", "sendAnimation", "sendAudio", "sendVoice", "sendSticker",
			"sendMediaGroup", "copyMessage", "forwardMessage",
		}},
		{methodOther, "other", []string{
			"getChat", "getChatMember", "getChatAdministrators", "leaveChat", "deleteMessage", "unknownMethod", "SendMessage", "",
		}},
	}
	for _, tt := range tests {
		t.Run(tt.label, func(t *testing.T) {
			for _, method := range tt.methods {
				class := classifyMethod(method)
				if class != tt.class || class.String() != tt.label {
					t.Errorf("classifyMethod(%q) = %s (%d), want %s (%d)", method, class, class, tt.label, tt.class)
				}
			}
		})
	}
}

func TestMethodClassBucketKind(t *testing.T) {
	tests := []struct {
		class methodClass
		want  [3]chatBucketKind
	}{
		{methodJoinRequest, [3]chatBucketKind{noChatBucket, noChatBucket, noChatBucket}},
		{methodMessage, [3]chatBucketKind{groupChatBucket, noChatBucket, privateChatBucket}},
		{methodOther, [3]chatBucketKind{noChatBucket, noChatBucket, noChatBucket}},
	}
	for _, tt := range tests {
		t.Run(tt.class.String(), func(t *testing.T) {
			for i, chatID := range [...]int64{-42, 0, 42} {
				if got := tt.class.bucketKind(chatID); got != tt.want[i] {
					t.Errorf("bucketKind(%d) = %d, want %d", chatID, got, tt.want[i])
				}
			}
		})
	}
}
