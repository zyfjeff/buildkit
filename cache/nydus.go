package cache

import (
	"compress/gzip"
	"context"
	"encoding/json"
	"fmt"
	"io"

	"github.com/containerd/containerd/content"
	"github.com/containerd/containerd/errdefs"
	"github.com/moby/buildkit/session"
	"github.com/moby/buildkit/util/compression"
	digest "github.com/opencontainers/go-digest"
	ocispecs "github.com/opencontainers/image-spec/specs-go/v1"
	"github.com/pkg/errors"

	nydusify "github.com/containerd/nydus-snapshotter/pkg/converter"
)

var nydusAnnotations = []string{nydusify.LayerAnnotationNydusBlob, nydusify.LayerAnnotationNydusBootstrap, nydusify.LayerAnnotationNydusBlobIDs}

func isNydusBlob(desc ocispecs.Descriptor) bool {
	if desc.Annotations == nil {
		return false
	}
	_, ok := desc.Annotations[nydusify.LayerAnnotationNydusBlob]
	return ok
}

func compressNydus(ctx context.Context, comp compression.Config) (compressor, func(context.Context, content.Store) (map[string]string, error)) {
	return func(dest io.Writer, requiredMediaType string) (io.WriteCloser, error) {
			return nydusify.Convert(ctx, dest, nydusify.ConvertOption{})
		}, func(ctx context.Context, cs content.Store) (map[string]string, error) {
			annotations := map[string]string{
				nydusify.LayerAnnotationNydusBlob: "true",
			}
			return annotations, nil
		}
}

func mergeNydus(ctx context.Context, refs []*immutableRef, comp compression.Config, s session.Group) (*ocispecs.Descriptor, error) {
	layers := []nydusify.Layer{}
	if len(refs) == 0 {
		return nil, fmt.Errorf("refs can't be empty")
	}
	var cm *cacheManager
	blobIDs := []string{}
	for _, ref := range refs {
		blobDesc, err := getBlobWithCompressionWithRetry(ctx, ref, comp, s)
		if err != nil {
			return nil, errors.Wrapf(err, "get compression blob %q", comp.Type)
		}
		ra, err := ref.cm.ContentStore.ReaderAt(ctx, blobDesc)
		if err != nil {
			return nil, errors.Wrapf(err, "get reader for compression blob %q", comp.Type)
		}
		defer ra.Close()
		if cm == nil {
			cm = ref.cm
		}
		blobIDs = append(blobIDs, blobDesc.Digest.Hex())
		layers = append(layers, nydusify.Layer{
			Digest:   blobDesc.Digest,
			ReaderAt: ra,
		})
	}

	chainID := refs[len(refs)-1].getChainID()
	cw, err := content.OpenWriter(ctx, cm.ContentStore, content.WithRef("nydus-merge-"+chainID.String()))
	if err != nil {
		return nil, errors.Wrap(err, "open content store writer")
	}
	defer cw.Close()

	pr, pw := io.Pipe()
	gw := gzip.NewWriter(cw)

	go func() {
		defer pw.Close()
		if err := nydusify.Merge(ctx, layers, pw, nydusify.MergeOption{
			WithTar: true,
		}); err != nil {
			pw.CloseWithError(errors.Wrapf(err, "merge nydus bootstrap"))
		}
	}()

	uncompressedDgst := digest.SHA256.Digester()
	compressed := io.MultiWriter(gw, uncompressedDgst.Hash())
	if _, err := io.Copy(compressed, pr); err != nil {
		return nil, errors.Wrapf(err, "copy bootstrap targz into content store")
	}
	if err := gw.Close(); err != nil {
		return nil, errors.Wrap(err, "close gzip writer")
	}

	labels := map[string]string{
		containerdUncompressed: uncompressedDgst.Digest().String(),
	}
	compressedDgst := cw.Digest()
	if err := cw.Commit(ctx, 0, compressedDgst, content.WithLabels(labels)); err != nil {
		if !errdefs.IsAlreadyExists(err) {
			return nil, errors.Wrap(err, "commit to content store")
		}
	}
	if err := cw.Close(); err != nil {
		return nil, errors.Wrap(err, "close content store writer")
	}

	info, err := cm.ContentStore.Info(ctx, compressedDgst)
	if err != nil {
		return nil, errors.Wrap(err, "get info from content store")
	}

	blobIDsBytes, err := json.Marshal(blobIDs)
	if err != nil {
		return nil, errors.Wrap(err, "marshal blob ids")
	}

	desc := ocispecs.Descriptor{
		Digest:    compressedDgst,
		Size:      info.Size,
		MediaType: ocispecs.MediaTypeImageLayerGzip,
		Annotations: map[string]string{
			// Use `containerd.io/uncompressed` to generate DiffID of
			// layer defined in OCI spec.
			containerdUncompressed:                 uncompressedDgst.Digest().String(),
			nydusify.LayerAnnotationNydusBootstrap: "true",
			nydusify.LayerAnnotationNydusBlobIDs:   string(blobIDsBytes),
		},
	}

	return &desc, nil
}
