package dispatch

import (
	"math"
	"testing"

	"github.com/go-telegram/bot/models"
)

func TestOrderAfter(t *testing.T) {
	for _, tc := range []struct {
		name      string
		event     Order
		watermark Order
		want      bool
	}{
		{name: "same date with higher ID", event: Order{100, 11}, watermark: Order{100, 10}, want: true},
		{name: "same date with lower ID", event: Order{100, 9}, watermark: Order{100, 10}},
		{name: "earlier date within 24 hours with higher ID", event: Order{100, 11}, watermark: Order{101, 10}, want: true},
		{name: "later date within 24 hours with lower ID", event: Order{101, 9}, watermark: Order{100, 10}},
		{name: "later date within 24 hours with equal ID", event: Order{101, 10}, watermark: Order{100, 10}},
		{name: "exactly 24 hours later with lower ID", event: Order{86500, 9}, watermark: Order{100, 10}},
		{name: "exactly 24 hours earlier with higher ID", event: Order{100, 11}, watermark: Order{86500, 10}, want: true},
		{name: "24 hours and one second later with lower ID", event: Order{86501, 9}, watermark: Order{100, 10}, want: true},
		{name: "24 hours and one second earlier with higher ID", event: Order{100, 11}, watermark: Order{86501, 10}},
		{name: "after an idle week with a lower ID", event: Order{604901, 1}, watermark: Order{100, 1000000}, want: true},
		{name: "after an idle week with the same ID", event: Order{604901, 42}, watermark: Order{100, 42}, want: true},
		{name: "duplicate", event: Order{100, 10}, watermark: Order{100, 10}},
		{name: "24 hours across zero", event: Order{-43200, 11}, watermark: Order{43200, 10}, want: true},
		{name: "24 hours near minimum date", event: Order{math.MinInt64, 11}, watermark: Order{math.MinInt64 + 86400, 10}, want: true},
		{name: "24 hours near maximum date", event: Order{math.MaxInt64, 9}, watermark: Order{math.MaxInt64 - 86400, 10}},
		{name: "minimum to maximum date", event: Order{math.MaxInt64, 9}, watermark: Order{math.MinInt64, 10}, want: true},
		{name: "maximum to minimum date", event: Order{math.MinInt64, 11}, watermark: Order{math.MaxInt64, 10}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := tc.event.After(tc.watermark); got != tc.want {
				t.Errorf("%+v.After(%+v) = %t, want %t", tc.event, tc.watermark, got, tc.want)
			}
		})
	}
}

func TestOrderOf(t *testing.T) {
	member := &models.ChatMemberUpdated{Date: 123}
	join := &models.ChatJoinRequest{Date: 456}
	message := &models.Message{Date: 789}
	for _, tc := range []struct {
		name   string
		update *models.Update
		want   Order
		ok     bool
	}{
		{name: "my_chat_member", update: &models.Update{ID: 1 << 40, MyChatMember: member}, want: Order{123, 1 << 40}, ok: true},
		{name: "chat_join_request", update: &models.Update{ID: 43, ChatJoinRequest: join}, want: Order{456, 43}, ok: true},
		{name: "nil update"},
		{name: "nil payload", update: &models.Update{ID: 44}},
		{name: "message", update: &models.Update{ID: 45, Message: message}},
		{name: "chat_member", update: &models.Update{ID: 46, ChatMember: member}},
		{name: "message with my_chat_member", update: &models.Update{ID: 47, Message: message, MyChatMember: member}},
		{name: "my_chat_member with chat_member", update: &models.Update{ID: 48, MyChatMember: member, ChatMember: member}},
		{name: "chat_join_request with chat_member", update: &models.Update{ID: 49, ChatJoinRequest: join, ChatMember: member}},
		{name: "both supported payloads", update: &models.Update{ID: 50, MyChatMember: member, ChatJoinRequest: join}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got, ok := OrderOf(tc.update)
			if got != tc.want || ok != tc.ok {
				t.Errorf("OrderOf = (%+v, %t), want (%+v, %t)", got, ok, tc.want, tc.ok)
			}
		})
	}
}
