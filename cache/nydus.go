package cache

import (
	"context"
	"os"

	"github.com/containerd/containerd/content"
	"github.com/containerd/containerd/errdefs"
	"github.com/containerd/containerd/mount"
	digest "github.com/opencontainers/go-digest"
	ocispecs "github.com/opencontainers/image-spec/specs-go/v1"
	"github.com/pkg/errors"

	"github.com/moby/buildkit/session"
	"github.com/moby/buildkit/util/compression"

	"github.com/goharbor/acceleration-service/pkg/driver/nydus/backend"
	"github.com/goharbor/acceleration-service/pkg/driver/nydus/packer"
)

const keyCachedNydusBootstrap = "cache.nydus-bootstrap"
const keyCachedNydusBlob = "cache.nydus-blob"

const valueCachedEmptySha256 = "sha256:none"

type buildLayer struct {
	sr *immutableRef
	sg session.Group
}

func isEmptyNydusBlob(sr *immutableRef) bool {
	return sr.GetString(keyCachedNydusBlob) == valueCachedEmptySha256
}

func (layer *buildLayer) getArtifact(ctx context.Context, dgst string, compressionType compression.Type) (*ocispecs.Descriptor, error) {
	artifactDigest := digest.Digest(dgst)
	if artifactDigest.Validate() != nil {
		return nil, errdefs.ErrNotFound
	}

	artifactDesc, err := layer.sr.getBlobWithCompression(ctx, compressionType)
	if err != nil {
		return nil, errdefs.ErrNotFound
	}

	return &artifactDesc, nil
}

// GetCache get nydus bootstrap/blob descriptor from cache,
// following situations should be returned:
// err != nil, cache miss;
// err == nil, cache hits;
func (layer *buildLayer) GetCache(ctx context.Context) (*ocispecs.Descriptor, []ocispecs.Descriptor, error) {
	bootstrapDesc, err := layer.getArtifact(ctx, layer.sr.GetString(keyCachedNydusBootstrap), compression.NydusBootstrap)
	if err != nil {
		return nil, nil, errdefs.ErrNotFound
	}

	cachedBlob := layer.sr.GetString(keyCachedNydusBlob)
	if cachedBlob == "" {
		return nil, nil, errdefs.ErrNotFound
	}

	if cachedBlob == valueCachedEmptySha256 {
		return bootstrapDesc, nil, nil
	}

	blobDesc, err := layer.getArtifact(ctx, cachedBlob, compression.NydusBlob)
	if err != nil {
		return nil, nil, errdefs.ErrNotFound
	}

	return bootstrapDesc, []ocispecs.Descriptor{*blobDesc}, nil
}

// SetCache records nydus bootstrap/blob descriptor to cache.
func (layer *buildLayer) SetCache(ctx context.Context, bootstrapDesc ocispecs.Descriptor, blobDescs []ocispecs.Descriptor) error {
	// Try to cache nydus bootstrap if no other compression type is set.
	if err := layer.sr.setBlob(ctx, bootstrapDesc); err != nil {
		return errors.Wrap(err, "set nydus bootstrap")
	}
	// Make sure to cache nydus bootstrap if other compression type has been set by setBlob.
	if err := layer.sr.linkBlob(ctx, bootstrapDesc); err != nil {
		return errors.Wrap(err, "set nydus blob")
	}
	layer.sr.SetString(keyCachedNydusBootstrap, bootstrapDesc.Digest.String(), "")

	// Cache nydus blob layer, the nydus blob of this layer may be empty.
	if len(blobDescs) > 0 {
		if err := layer.sr.linkBlob(ctx, blobDescs[0]); err != nil {
			return errors.Wrap(err, "set nydus blob")
		}
		layer.sr.SetString(keyCachedNydusBlob, blobDescs[0].Digest.String(), "")
	} else {
		layer.sr.SetString(keyCachedNydusBlob, valueCachedEmptySha256, "")
	}

	return nil
}

func (layer *buildLayer) ContentStore(ctx context.Context) content.Store {
	return layer.sr.cm.ContentStore
}

func (layer *buildLayer) Mount(ctx context.Context) ([]mount.Mount, func() error, error) {
	mountable, err := layer.sr.Mount(ctx, true, layer.sg)
	if err != nil {
		return nil, nil, err
	}

	return mountable.Mount()
}

func (layer *buildLayer) Backend(ctx context.Context) backend.Backend {
	return nil
}

func computeNydusBlobChain(ctx context.Context, sr *immutableRef, sg session.Group) ([]ocispecs.Descriptor, error) {
	layers := []packer.Layer{}

	sr.layerWalk(func(sr *immutableRef) {
		layer := &buildLayer{
			sr: sr,
			sg: sg,
		}
		layers = append(layers, layer)
	})

	builder := os.Getenv("NYDUS_BUILDER")
	if builder == "" {
		builder = "nydus-image"
	}
	p, err := packer.New(packer.Option{
		WorkDir:     "",
		BuilderPath: builder,
	})
	if err != nil {
		return nil, errors.Wrap(err, "create nydus packer")
	}

	descs, err := p.Build(ctx, nil, layers)
	if err != nil {
		return nil, errors.Wrap(err, "build nydus layers")
	}

	extraDescs := []ocispecs.Descriptor{}
	for idx := range descs {
		if err := func(idx int) error {
			desc := descs[idx]

			// The last layer of nydus image is bootstrap, so it needs to be appended with.
			if idx == len(layers)-1 {
				extraDescs = append(extraDescs, desc.Bootstrap)
			}

			return nil
		}(idx); err != nil {
			return nil, err
		}
	}

	return extraDescs, nil
}
