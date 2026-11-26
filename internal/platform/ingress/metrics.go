package ingress

import (
	"time"

	"github.com/INKCR0W/yabot/internal/platform/dispatch"
	"github.com/VictoriaMetrics/metrics"
	"github.com/go-telegram/bot/models"
)

const (
	batchWindowSize = 100
	unknownKind     = ^dispatch.Kind(0)
)

type ingressMetrics struct {
	set            *metrics.Set
	lagSeconds     *metrics.Histogram
	settleSeconds  *metrics.Histogram
	batchSize      *metrics.Histogram
	fullRatio      *metrics.Gauge
	poison         *metrics.Counter
	deadLetters    *metrics.Counter
	pollRetries    *metrics.Counter
	webhookRetries *metrics.Counter

	fullBatches [batchWindowSize]bool
	nextBatch   int
	batchCount  int
	fullCount   int
}

func newIngressMetrics(set *metrics.Set) ingressMetrics {
	if set == nil {
		return ingressMetrics{}
	}
	return ingressMetrics{
		set:            set,
		lagSeconds:     set.GetOrCreateHistogram("yabot_ingress_lag_seconds"),
		settleSeconds:  set.GetOrCreateHistogram("yabot_ingress_settle_seconds"),
		batchSize:      set.GetOrCreateHistogram("yabot_ingress_batch_size"),
		fullRatio:      set.GetOrCreateGauge("yabot_ingress_full_batch_ratio", nil),
		poison:         set.GetOrCreateCounter("yabot_ingress_poison_total"),
		deadLetters:    set.GetOrCreateCounter("yabot_ingress_dead_letters_total"),
		pollRetries:    set.GetOrCreateCounter("yabot_ingress_poll_retries_total"),
		webhookRetries: set.GetOrCreateCounter("yabot_ingress_delete_webhook_retries_total"),
	}
}

func (m *ingressMetrics) observeBatch(updates []models.Update, now func() time.Time, skew Skew) {
	if m.set == nil {
		return
	}
	recvWall := now()
	size := len(updates)
	m.batchSize.Update(float64(size))
	if m.fullBatches[m.nextBatch] {
		m.fullCount--
	}
	full := size == BatchLimit
	if full {
		m.fullCount++
	}
	m.fullBatches[m.nextBatch] = full
	m.nextBatch = (m.nextBatch + 1) % len(m.fullBatches)
	m.batchCount = min(m.batchCount+1, len(m.fullBatches))
	m.fullRatio.Set(float64(m.fullCount) / float64(m.batchCount))

	for idx := range updates {
		update := &updates[idx]
		name, kind := updateKind(update)
		m.set.GetOrCreateCounter(`yabot_updates_total{kind="` + name + `"}`).Inc()
		if date := updateDate(update, kind); date != 0 {
			m.lagSeconds.Update(skew.Age(recvWall, date).Seconds())
		}
	}
}

func updateKind(update *models.Update) (string, dispatch.Kind) {
	kind, ok := dispatch.KindOf(update)
	if !ok {
		return "unknown", unknownKind
	}
	return kind.String(), kind
}

func updateDate(update *models.Update, kind dispatch.Kind) int64 {
	switch kind {
	case dispatch.KindMessage:
		return int64(update.Message.Date)
	case dispatch.KindEditedMessage:
		return int64(update.EditedMessage.Date)
	case dispatch.KindChannelPost:
		return int64(update.ChannelPost.Date)
	case dispatch.KindEditedChannelPost:
		return int64(update.EditedChannelPost.Date)
	case dispatch.KindBusinessConnection:
		return update.BusinessConnection.Date
	case dispatch.KindBusinessMessage:
		return int64(update.BusinessMessage.Date)
	case dispatch.KindEditedBusinessMessage:
		return int64(update.EditedBusinessMessage.Date)
	case dispatch.KindMessageReaction:
		return int64(update.MessageReaction.Date)
	case dispatch.KindMessageReactionCount:
		return int64(update.MessageReactionCount.Date)
	case dispatch.KindGuestMessage:
		return int64(update.GuestMessage.Date)
	case dispatch.KindChatJoinRequest:
		return int64(update.ChatJoinRequest.Date)
	case dispatch.KindMyChatMember:
		return int64(update.MyChatMember.Date)
	case dispatch.KindChatMember:
		return int64(update.ChatMember.Date)
	case dispatch.KindChatBoost:
		return int64(update.ChatBoost.Boost.AddDate)
	case dispatch.KindRemovedChatBoost:
		return int64(update.RemovedChatBoost.RemoveDate)
	default:
		return 0
	}
}
