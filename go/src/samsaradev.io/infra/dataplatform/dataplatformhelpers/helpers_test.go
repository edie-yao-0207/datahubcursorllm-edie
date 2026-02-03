package dataplatformhelpers

import (
	"reflect"
	"testing"
)

func TestVPC_getSupportedAzs(t *testing.T) {

	type args struct {
		azs []string
	}
	tests := []struct {
		name    string
		args    args
		want    []string
		wantErr bool
	}{
		{
			name: "testValidAzs",
			args: args{
				azs: []string{"ca-central-1a", "ca-central-1b", "ca-central-1d"},
			},
			want: []string{"a", "b", "d"},
		},

		{
			name: "testInValidAzs",
			args: args{
				azs: []string{"ca-central-1a", "ca-central-1b", "ca-central-1z"}, // ca-central-1z is not a valid AZ, only supported are a,b,c,d.
			},
			wantErr: true,
		},

		{
			name: "testInValidAzs",
			args: args{
				azs: []string{"", "ca-central-1b", "ca-central-1c"}, // "" is not a valid AZ, only supported are a,b,c,d.
			},
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {

			got, err := GetSupportedAzs(tt.args.azs)
			if (err != nil) != tt.wantErr {
				t.Errorf("GetSupportedAzs() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("GetSupportedAzs() = %v, want %v", got, tt.want)
			}
		})
	}
}
