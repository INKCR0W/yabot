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
	switch method {
	case "sendChatJoinRequestWebApp", "answerChatJoinRequestQuery", "approveChatJoinRequest", "declineChatJoinRequest":
		return methodJoinRequest
	case "sendMessage", "editMessageText", "editMessageReplyMarkup", "editMessageCaption", "editMessageMedia",
		"sendPhoto", "sendDocument", "sendVideo", "sendAnimation", "sendAudio", "sendVoice", "sendSticker",
		"sendMediaGroup", "copyMessage", "forwardMessage":
		return methodMessage
	default:
		return methodOther
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
