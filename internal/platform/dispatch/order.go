package dispatch

import "github.com/go-telegram/bot/models"

// Order records an event's Telegram timestamp in seconds and its update_id for watermark comparisons.
type Order struct{ Date, ID int64 }

// OrderOf returns false unless u has exactly one payload of type my_chat_member or chat_join_request.
func OrderOf(u *models.Update) (Order, bool) {
	kind, ok := KindOf(u)
	if !ok {
		return Order{}, false
	}

	switch kind {
	case KindMyChatMember:
		if *u == (models.Update{ID: u.ID, MyChatMember: u.MyChatMember}) {
			return Order{Date: int64(u.MyChatMember.Date), ID: u.ID}, true
		}
	case KindChatJoinRequest:
		if *u == (models.Update{ID: u.ID, ChatJoinRequest: u.ChatJoinRequest}) {
			return Order{Date: int64(u.ChatJoinRequest.Date), ID: u.ID}, true
		}
	}
	return Order{}, false
}

// After compares a new event o with the applied watermark p.
// It does not define a total order and must not be used for sorting.
func (o Order) After(p Order) bool {
	const orderIDWindowSeconds = 24 * 60 * 60
	earlier, later := p.Date, o.Date
	if earlier > later {
		earlier, later = later, earlier
	}
	// The nonnegative difference fits in uint64 across the full int64 range.
	if uint64(later)-uint64(earlier) <= orderIDWindowSeconds {
		return o.ID > p.ID
	}
	return o.Date > p.Date
}
