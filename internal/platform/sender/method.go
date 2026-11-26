package sender

type methodClass uint8

const (
	methodJoinRequest methodClass = iota
	methodMessage
	methodOther
	methodClassCount
)

func (class methodClass) String() string {
	switch class {
	case methodJoinRequest:
		return "join_request"
	case methodMessage:
		return "message"
	case methodOther:
		return "other"
	default:
		return "unknown"
	}
}

func classifyMethod(method string) methodClass {
	class, _ := methodInfo(method)
	return class
}

func methodInfo(method string) (methodClass, string) {
	switch method {
	case "sendChatJoinRequestWebApp", "answerChatJoinRequestQuery", "approveChatJoinRequest", "declineChatJoinRequest":
		return methodJoinRequest, method
	case "sendMessage", "editMessageText", "editMessageReplyMarkup", "editMessageCaption", "editMessageMedia",
		"sendPhoto", "sendDocument", "sendVideo", "sendAnimation", "sendAudio", "sendVoice", "sendSticker",
		"sendMediaGroup", "copyMessage", "forwardMessage":
		return methodMessage, method
	case "getChat", "getChatMember", "getChatAdministrators", "leaveChat", "deleteMessage":
		return methodOther, method
	default:
		return methodOther, "other"
	}
}

type chatBucketKind uint8

const (
	noChatBucket chatBucketKind = iota
	groupChatBucket
	privateChatBucket
)

func (class methodClass) bucketKind(chatID int64) chatBucketKind {
	switch {
	case class != methodMessage || chatID == 0:
		return noChatBucket
	case chatID < 0:
		return groupChatBucket
	default:
		return privateChatBucket
	}
}
