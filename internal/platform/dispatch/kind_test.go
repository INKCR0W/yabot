package dispatch

import (
	"reflect"
	"strings"
	"testing"

	"github.com/go-telegram/bot/models"
)

func TestKindOfUpdatePayloads(t *testing.T) {
	updateType := reflect.TypeFor[models.Update]()
	var seen KindMask
	for i := range updateType.NumField() {
		field := updateType.Field(i)
		name, _, _ := strings.Cut(field.Tag.Get("json"), ",")
		if name == "update_id" {
			continue
		}
		t.Run(name, func(t *testing.T) {
			if field.Type.Kind() != reflect.Pointer {
				t.Fatalf("models.Update.%s has type %v, expected a pointer payload", field.Name, field.Type)
			}
			update := &models.Update{ID: 42}
			reflect.ValueOf(update).Elem().Field(i).Set(reflect.New(field.Type.Elem()))
			kind, ok := KindOf(update)
			if !ok {
				t.Fatal("KindOf did not recognize the payload")
			}
			if got := kind.String(); got != name {
				t.Errorf("KindOf returned kind %d with name %q, want %q", kind, got, name)
			}
			mask := kind.Mask()
			if want := KindMask(1) << kind; mask == 0 || mask != want || seen&mask != 0 {
				t.Errorf("kind %d returned mask %#x, want unused bit %#x", kind, mask, want)
			}
			seen |= mask
		})
	}
	if want := KindMask(1<<kindCount - 1); seen != want {
		t.Errorf("payload masks = %#x, want all declared Kind bits %#x", seen, want)
	}
}

func TestKindOfWithoutPayload(t *testing.T) {
	for _, tc := range []struct {
		name   string
		update *models.Update
	}{
		{name: "nil"},
		{name: "ID only", update: &models.Update{ID: 42}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if kind, ok := KindOf(tc.update); ok {
				t.Errorf("KindOf = (%v, true), want false", kind)
			}
		})
	}
}

func TestKindOfMultiplePayloads(t *testing.T) {
	update := &models.Update{ChatJoinRequest: &models.ChatJoinRequest{}, MyChatMember: &models.ChatMemberUpdated{}}
	if kind, ok := KindOf(update); kind != KindChatJoinRequest || !ok {
		t.Errorf("KindOf = (%v, %t), want (%v, true)", kind, ok, KindChatJoinRequest)
	}
}

func TestUnknownKind(t *testing.T) {
	for _, kind := range []Kind{kindCount, 255} {
		if got := kind.Mask(); got != 0 {
			t.Errorf("Kind(%d).Mask() = %#x, want zero", kind, got)
		}
		if got := kind.String(); got != "" {
			t.Errorf("Kind(%d).String() = %q, want empty string", kind, got)
		}
	}
}
