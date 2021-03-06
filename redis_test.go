package redis

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/go-redis/redis/v8"
	"github.com/unistack-org/micro/v3/store"
)

func Test_rkv_configure(t *testing.T) {
	type fields struct {
		options store.Options
		Client  *redis.Client
	}
	type wantValues struct {
		username string
		password string
		address  string
	}

	tests := []struct {
		name    string
		fields  fields
		wantErr bool
		want    wantValues
	}{
		{name: "No Url", fields: fields{options: store.Options{}, Client: nil},
			wantErr: false, want: wantValues{
				username: "",
				password: "",
				address:  "127.0.0.1:6379",
			}},
		{name: "legacy Url", fields: fields{options: store.Options{Nodes: []string{"127.0.0.1:6379"}}, Client: nil},
			wantErr: false, want: wantValues{
				username: "",
				password: "",
				address:  "127.0.0.1:6379",
			}},
		{name: "New Url", fields: fields{options: store.Options{Nodes: []string{"redis://127.0.0.1:6379"}}, Client: nil},
			wantErr: false, want: wantValues{
				username: "",
				password: "",
				address:  "127.0.0.1:6379",
			}},
		{name: "Url with Pwd", fields: fields{options: store.Options{Nodes: []string{"redis://:password@redis:6379"}}, Client: nil},
			wantErr: false, want: wantValues{
				username: "",
				password: "password",
				address:  "redis:6379",
			}},
		{name: "Url with username and Pwd", fields: fields{options: store.Options{Nodes: []string{"redis://username:password@redis:6379"}}, Client: nil},
			wantErr: false, want: wantValues{
				username: "username",
				password: "password",
				address:  "redis:6379",
			}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := &rkv{
				opts: tt.fields.options,
				cli:  tt.fields.Client,
			}
			err := r.configure()
			if (err != nil) != tt.wantErr {
				t.Errorf("configure() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
		})
	}
}

func Test_Store(t *testing.T) {
	ctx := context.Background()

	if tr := os.Getenv("INTEGRATION_TESTS"); len(tr) > 0 {
		t.Skip()
	}
	r := new(rkv)

	//r.options = store.Options{Nodes: []string{"redis://:password@127.0.0.1:6379"}}
	//r.options = store.Options{Nodes: []string{"127.0.0.1:6379"}}

	r.opts = store.NewOptions(store.Nodes(os.Getenv("STORE_NODES")))

	if err := r.configure(); err != nil {
		t.Fatal(err)
	}

	key := "myTest"
	var val []byte
	err := r.Write(ctx, key, []byte("myValue"), store.WriteTTL(2*time.Minute))
	if err != nil {
		t.Fatalf("Write error: %v", err)
	}
	err = r.Read(ctx, key, val)
	if err != nil {
		t.Fatalf("Read error: %v\n", err)
	}
	err = r.Delete(ctx, key)
	if err != nil {
		t.Fatalf("Delete error: %v\n", err)
	}
	_, err = r.List(ctx)
	if err != nil {
		t.Fatalf("List error: %v\n", err)
	}
}
