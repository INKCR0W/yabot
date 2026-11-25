package dispatch

import "github.com/go-telegram/bot/models"

type Kind uint8

type KindMask uint64

const (
	KindMessage Kind = iota
	KindEditedMessage
	KindChannelPost
	KindEditedChannelPost
	KindBusinessConnection
	KindBusinessMessage
	KindEditedBusinessMessage
	KindDeletedBusinessMessages
	KindMessageReaction
	KindMessageReactionCount
	KindInlineQuery
	KindChosenInlineResult
	KindCallbackQuery
	KindWidgetQuery
	KindSharedWidgetQuery
	KindShippingQuery
	KindPreCheckoutQuery
	KindPurchasedPaidMedia
	KindPoll
	KindPollAnswer
	KindManagedBot
	KindGuestMessage
	KindChatJoinRequest
	KindMyChatMember
	KindChatMember
	KindChatBoost
	KindRemovedChatBoost
	KindSubscription
	KindStoppedMessageGeneration
	kindCount
)

var kindNames = [kindCount]string{
	KindMessage:                  models.AllowedUpdateMessage,
	KindEditedMessage:            models.AllowedUpdateEditedMessage,
	KindChannelPost:              models.AllowedUpdateChannelPost,
	KindEditedChannelPost:        models.AllowedUpdateEditedChannelPost,
	KindBusinessConnection:       models.AllowedUpdateBusinessConnection,
	KindBusinessMessage:          models.AllowedUpdateBusinessMessage,
	KindEditedBusinessMessage:    models.AllowedUpdateEditedBusinessMessage,
	KindDeletedBusinessMessages:  models.AllowedUpdateDeletedBusinessMessages,
	KindMessageReaction:          models.AllowedUpdateMessageReaction,
	KindMessageReactionCount:     models.AllowedUpdateMessageReactionCount,
	KindInlineQuery:              models.AllowedUpdateInlineQuery,
	KindChosenInlineResult:       models.AllowedUpdateChosenInlineResult,
	KindCallbackQuery:            models.AllowedUpdateCallbackQuery,
	KindWidgetQuery:              models.AllowedUpdateWidgetQuery,
	KindSharedWidgetQuery:        models.AllowedUpdateSharedWidgetQuery,
	KindShippingQuery:            models.AllowedUpdateShippingQuery,
	KindPreCheckoutQuery:         models.AllowedUpdatePreCheckoutQuery,
	KindPurchasedPaidMedia:       models.AllowedUpdatePurchasedPaidMedia,
	KindPoll:                     models.AllowedUpdatePoll,
	KindPollAnswer:               models.AllowedUpdatePollAnswer,
	KindManagedBot:               models.AllowedUpdateManagedBot,
	KindGuestMessage:             models.AllowedUpdateGuestMessage,
	KindChatJoinRequest:          models.AllowedUpdateChatJoinRequest,
	KindMyChatMember:             models.AllowedUpdateMyChatMember,
	KindChatMember:               models.AllowedUpdateChatMember,
	KindChatBoost:                models.AllowedUpdateChatBoost,
	KindRemovedChatBoost:         models.AllowedUpdateRemovedChatBoost,
	KindSubscription:             models.AllowedUpdateSubscription,
	KindStoppedMessageGeneration: models.AllowedUpdateStoppedMessageGeneration,
}

// Mask returns zero for an unknown Kind.
func (k Kind) Mask() KindMask {
	if k >= kindCount {
		return 0
	}
	return 1 << k
}

// String returns the Telegram allowed_updates name, or an empty string for an unknown Kind.
func (k Kind) String() string {
	if k >= kindCount {
		return ""
	}
	return kindNames[k]
}

// KindOf returns false for a nil update or an update without a payload.
// If multiple payloads are set, it returns the first in Kind order.
func KindOf(u *models.Update) (Kind, bool) {
	switch {
	case u == nil:
		return 0, false
	case u.Message != nil:
		return KindMessage, true
	case u.EditedMessage != nil:
		return KindEditedMessage, true
	case u.ChannelPost != nil:
		return KindChannelPost, true
	case u.EditedChannelPost != nil:
		return KindEditedChannelPost, true
	case u.BusinessConnection != nil:
		return KindBusinessConnection, true
	case u.BusinessMessage != nil:
		return KindBusinessMessage, true
	case u.EditedBusinessMessage != nil:
		return KindEditedBusinessMessage, true
	case u.DeletedBusinessMessages != nil:
		return KindDeletedBusinessMessages, true
	case u.MessageReaction != nil:
		return KindMessageReaction, true
	case u.MessageReactionCount != nil:
		return KindMessageReactionCount, true
	case u.InlineQuery != nil:
		return KindInlineQuery, true
	case u.ChosenInlineResult != nil:
		return KindChosenInlineResult, true
	case u.CallbackQuery != nil:
		return KindCallbackQuery, true
	case u.WidgetQuery != nil:
		return KindWidgetQuery, true
	case u.SharedWidgetQuery != nil:
		return KindSharedWidgetQuery, true
	case u.ShippingQuery != nil:
		return KindShippingQuery, true
	case u.PreCheckoutQuery != nil:
		return KindPreCheckoutQuery, true
	case u.PurchasedPaidMedia != nil:
		return KindPurchasedPaidMedia, true
	case u.Poll != nil:
		return KindPoll, true
	case u.PollAnswer != nil:
		return KindPollAnswer, true
	case u.ManagedBot != nil:
		return KindManagedBot, true
	case u.GuestMessage != nil:
		return KindGuestMessage, true
	case u.ChatJoinRequest != nil:
		return KindChatJoinRequest, true
	case u.MyChatMember != nil:
		return KindMyChatMember, true
	case u.ChatMember != nil:
		return KindChatMember, true
	case u.ChatBoost != nil:
		return KindChatBoost, true
	case u.RemovedChatBoost != nil:
		return KindRemovedChatBoost, true
	case u.Subscription != nil:
		return KindSubscription, true
	case u.StoppedMessageGeneration != nil:
		return KindStoppedMessageGeneration, true
	default:
		return 0, false
	}
}
