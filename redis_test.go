package redis

import (
	"bytes"
	"context"
	"os"
	"testing"
	"time"

	goredis "github.com/redis/go-redis/v9"
	"go.unistack.org/micro/v3/store"
	"go.unistack.org/micro/v3/tracer"
)

func TestKeepTTL(t *testing.T) {
	ctx := context.Background()

	if tr := os.Getenv("INTEGRATION_TESTS"); len(tr) > 0 {
		t.Skip()
	}
	r := NewStore(store.Addrs(os.Getenv("STORE_NODES")))

	if err := r.Init(); err != nil {
		t.Fatal(err)
	}
	if err := r.Connect(ctx); err != nil {
		t.Fatal(err)
	}

	key := "key"
	err := r.Write(ctx, key, "val1", store.WriteTTL(15*time.Second))
	if err != nil {
		t.Fatalf("Write error: %v", err)
	}
	time.Sleep(3 * time.Second)
	err = r.Write(ctx, key, "val2", store.WriteTTL(-1))
	if err != nil {
		t.Fatalf("Write error: %v", err)
	}
}

func Test_rkv_configure(t *testing.T) {
	type fields struct {
		options store.Options
		Client  goredis.UniversalClient
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
		{
			name: "No Url", fields: fields{options: store.Options{}, Client: nil},
			wantErr: false, want: wantValues{
				username: "",
				password: "",
				address:  "127.0.0.1:6379",
			},
		},
		{
			name: "legacy Url", fields: fields{options: store.Options{Tracer: tracer.DefaultTracer, Addrs: []string{"127.0.0.1:6379"}}, Client: nil},
			wantErr: false, want: wantValues{
				username: "",
				password: "",
				address:  "127.0.0.1:6379",
			},
		},
		{
			name: "New Url", fields: fields{options: store.Options{Tracer: tracer.DefaultTracer, Addrs: []string{"redis://127.0.0.1:6379"}}, Client: nil},
			wantErr: false, want: wantValues{
				username: "",
				password: "",
				address:  "127.0.0.1:6379",
			},
		},
		{
			name: "Url with Pwd", fields: fields{options: store.Options{Tracer: tracer.DefaultTracer, Addrs: []string{"redis://:password@redis:6379"}}, Client: nil},
			wantErr: false, want: wantValues{
				username: "",
				password: "password",
				address:  "redis:6379",
			},
		},
		{
			name: "Url with username and Pwd", fields: fields{options: store.Options{Tracer: tracer.DefaultTracer, Addrs: []string{"redis://username:password@redis:6379"}}, Client: nil},
			wantErr: false, want: wantValues{
				username: "username",
				password: "password",
				address:  "redis:6379",
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			rc := &Store{
				opts: tt.fields.options,
				cli:  tt.fields.Client,
			}
			err := rc.configure()
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
	r := NewStore(store.Addrs(os.Getenv("STORE_NODES")))

	if err := r.Init(); err != nil {
		t.Fatal(err)
	}
	if err := r.Connect(ctx); err != nil {
		t.Fatal(err)
	}

	key := "myTest"
	tval := []byte("myValue")
	var val []byte
	err := r.Write(ctx, key, tval, store.WriteTTL(2*time.Minute))
	if err != nil {
		t.Fatalf("Write error: %v", err)
	}
	err = r.Read(ctx, key, &val)
	if err != nil {
		t.Fatalf("Read error: %v\n", err)
	} else if !bytes.Equal(val, tval) {
		t.Fatalf("read err: data not eq")
	}
	keys, err := r.List(ctx)
	if err != nil {
		t.Fatalf("List error: %v\n", err)
	}
	_ = keys
	err = r.Delete(ctx, key)
	if err != nil {
		t.Fatalf("Delete error: %v\n", err)
	}
	// t.Logf("%v", keys)
}

func Test_MRead(t *testing.T) {
	ctx := context.Background()
	var err error
	if tr := os.Getenv("INTEGRATION_TESTS"); len(tr) > 0 {
		t.Skip()
	}
	r := NewStore(store.Addrs(os.Getenv("STORE_NODES")))

	if err = r.Init(); err != nil {
		t.Fatal(err)
	}
	if err = r.Connect(ctx); err != nil {
		t.Fatal(err)
	}

	key1 := "myTest1"
	key2 := "myTest2"
	tval1 := []byte("myValue1")
	tval2 := []byte("myValue2")
	var vals [][]byte
	err = r.Write(ctx, key1, tval1, store.WriteTTL(2*time.Minute))
	if err != nil {
		t.Fatalf("Write error: %v", err)
	}
	err = r.Write(ctx, key2, tval2, store.WriteTTL(2*time.Minute))
	if err != nil {
		t.Fatalf("Write error: %v", err)
	}
	err = r.MRead(ctx, []string{key1, key2}, &vals)
	if err != nil {
		t.Fatalf("Read error: %v\n", err)
	}
	// t.Logf("%s", vals)
	_ = vals
	keys, err := r.List(ctx)
	if err != nil {
		t.Fatalf("List error: %v\n", err)
	}
	_ = keys
	// t.Logf("%v", keys)
	err = r.Delete(ctx, key1)
	if err != nil {
		t.Fatalf("Delete error: %v\n", err)
	}
	err = r.Delete(ctx, key2)
	if err != nil {
		t.Fatalf("Delete error: %v\n", err)
	}
}

/*
func Test_MReadCodec(t *testing.T) {
	type mytype struct {
		Key string `json:"name"`
		Val string `json:"val"`
	}
	ctx := context.Background()
	var err error
	if tr := os.Getenv("INTEGRATION_TESTS"); len(tr) > 0 {
		t.Skip()
	}
	r := NewStore(store.Nodes(os.Getenv("STORE_NODES")), store.Codec(jsoncodec.NewCodec()))

	if err = r.Init(); err != nil {
		t.Fatal(err)
	}
	if err = r.Connect(ctx); err != nil {
		t.Fatal(err)
	}

	key1 := "myTest1"
	key2 := "myTest2"
	key3 := "myTest3"
	tval1 := &mytype{Key: "key1", Val: "val1"}
	tval2 := &mytype{Key: "key2", Val: "val2"}
	var vals []*mytype
	err = r.Write(ctx, key1, tval1, store.WriteTTL(2*time.Minute))
	if err != nil {
		t.Fatalf("Write error: %v", err)
	}
	err = r.Write(ctx, key2, tval2, store.WriteTTL(2*time.Minute))
	if err != nil {
		t.Fatalf("Write error: %v", err)
	}
	err = r.MRead(ctx, []string{key1, key3, key2}, &vals)
	if err != nil {
		t.Fatalf("Read error: %v\n", err)
	}
	if vals[0].Key != "key1" || vals[1] != nil || vals[2].Key != "key2" {
		t.Fatalf("read err: struct not filled")
	}
	keys, err := r.List(ctx)
	if err != nil {
		t.Fatalf("List error: %v\n", err)
	}
	_ = keys
	err = r.Delete(ctx, key1)
	if err != nil {
		t.Fatalf("Delete error: %v\n", err)
	}
	err = r.Delete(ctx, key2)
	if err != nil {
		t.Fatalf("Delete error: %v\n", err)
	}
}
*/
