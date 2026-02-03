package aws

import (
	"context"
	"encoding/json"
	"io"
	"strings"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/request"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/samsarahq/go/oops"

	"samsaradev.io/infra/samsaraaws/awshelpers/awssessions"
	"samsaradev.io/infra/samsaraaws/awsregionconsts"
)

type s3GetObjectAPI interface {
	GetObjectWithContext(aws.Context, *s3.GetObjectInput, ...request.Option) (*s3.GetObjectOutput, error)
}

// s3SecretsReader reads secrets from the same S3-backed secrets file used by secretshelper:
// - bucket: <regionPrefix><envSuffix> (e.g. samsara-eu-prod-app-configs)
// - object: .secrets.json (or secrets.json in dev)
//
// This is intentionally implemented inside dataplatops so we can:
// - Use a distinct shared-config profile for reading secrets (e.g. eu-readadmin)
// - Avoid subprocess/stdout parsing
// - Avoid importing infra/security/internal packages
type s3SecretsReader struct {
	region  string
	env     string
	profile string

	bucket string
	key    string

	s3 s3GetObjectAPI
}

var s3SecretsBucketSuffixByEnv = map[string]string{
	"prod":       "prod-app-configs",
	"build":      "build-configs",
	"dev":        "dev-configs",
	"playground": "playground-app-configs",
}

var s3SecretsObjectKeyByEnv = map[string]string{
	"prod":       ".secrets.json",
	"build":      ".secrets.json",
	"dev":        "secrets.json", // no leading dot in dev
	"playground": ".secrets.json",
}

func newS3SecretsReader(region, env, profile string) (secretReader, error) {
	region = strings.TrimSpace(region)
	env = strings.TrimSpace(env)
	profile = strings.TrimSpace(profile)
	if region == "" {
		return nil, oops.Errorf("region is required")
	}
	if env == "" {
		env = "prod"
	}

	suffix := s3SecretsBucketSuffixByEnv[env]
	objKey := s3SecretsObjectKeyByEnv[env]
	if suffix == "" || objKey == "" {
		return nil, oops.Errorf("invalid env %q", env)
	}

	prefix := awsregionconsts.RegionPrefix[region]
	if prefix == "" {
		return nil, oops.Errorf("unsupported region %q", region)
	}

	opts := session.Options{
		SharedConfigState: session.SharedConfigEnable,
		Profile:           profile,
	}
	opts.Config.MergeIn(&aws.Config{Region: aws.String(region)})
	sess := awssessions.NewInstrumentedAWSSessionWithOptions(opts)

	return &s3SecretsReader{
		region:  region,
		env:     env,
		profile: profile,
		bucket:  prefix + suffix,
		key:     objKey,
		s3:      s3.New(sess),
	}, nil
}

func (r *s3SecretsReader) Read(ctx context.Context, key string) (string, error) {
	key = strings.TrimSpace(key)
	if key == "" {
		return "", oops.Errorf("secret key is empty")
	}

	out, err := r.s3.GetObjectWithContext(ctx, &s3.GetObjectInput{
		Bucket: aws.String(r.bucket),
		Key:    aws.String(r.key),
	})
	if err != nil {
		return "", oops.Wrapf(err, "fetch secrets file from s3: bucket=%s key=%s", r.bucket, r.key)
	}
	defer out.Body.Close()

	body, err := io.ReadAll(out.Body)
	if err != nil {
		return "", oops.Wrapf(err, "read secrets file body: bucket=%s key=%s", r.bucket, r.key)
	}

	var parsed map[string]any
	if err := json.Unmarshal(body, &parsed); err != nil {
		return "", oops.Wrapf(err, "parse secrets json: bucket=%s key=%s", r.bucket, r.key)
	}

	val, ok := parsed[key]
	if !ok {
		return "", oops.Errorf("key %q not found in secrets file", key)
	}
	if val == nil {
		return "", oops.Errorf("key %q is null in secrets file", key)
	}
	s := strings.TrimSpace(toString(val))
	if s == "" {
		return "", oops.Errorf("key %q value is empty in secrets file", key)
	}
	return s, nil
}

func toString(v any) string {
	switch t := v.(type) {
	case nil:
		return ""
	case string:
		return t
	default:
		// Match secretshelper behavior: fmt.Sprintf("%v", val) without importing fmt.
		// For non-string values this will be JSON-marshaled best-effort.
		b, err := json.Marshal(t)
		if err != nil {
			return ""
		}
		// json.Marshal on a string adds quotes; handle that.
		s := string(b)
		return strings.Trim(s, "\"")
	}
}
